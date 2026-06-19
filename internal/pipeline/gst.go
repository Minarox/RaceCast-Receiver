package pipeline

// gst.go gère les pipelines GStreamer appsrc→…→appsink utilisés pour décoder
// les flux AV1 et Opus reçus via libsrt (listener.go).
//
// Les données arrivent par Push() dans l'appsrc "src".
// Les frames décodées sont lues par le consommateur via Frames().

// #cgo pkg-config: gstreamer-1.0 gstreamer-app-1.0
// #include <gst/gst.h>
// #include <gst/app/gstappsink.h>
// #include <gst/app/gstappsrc.h>
// #include <stdlib.h>
//
// // push_buffer copie data dans un GstBuffer et l'envoie dans l'appsrc.
// // gst_app_src_push_buffer prend la propriété du buffer (pas de free côté Go).
// static GstFlowReturn push_buffer(GstElement *src, const char *data, int len) {
//     GstBuffer *buf = gst_buffer_new_allocate(NULL, (gsize)len, NULL);
//     GstMapInfo m;
//     gst_buffer_map(buf, &m, GST_MAP_WRITE);
//     memcpy(m.data, data, (size_t)len);
//     gst_buffer_unmap(buf, &m);
//     return gst_app_src_push_buffer(GST_APP_SRC(src), buf);
// }
//
// // eos_appsrc envoie un événement EOS dans l'appsrc ; il se propage jusqu'à l'appsink.
// static void eos_appsrc(GstElement *src) {
//     gst_app_src_end_of_stream(GST_APP_SRC(src));
// }
//
// // Passe le pipeline en PLAYING et attend la fin de l'initialisation asynchrone.
// // Retourne 1 si succès, 0 sinon.
// static int start_pipeline(GstElement *pipeline) {
//     GstStateChangeReturn ret = gst_element_set_state(pipeline, GST_STATE_PLAYING);
//     if (ret == GST_STATE_CHANGE_ASYNC) {
//         GstState state;
//         ret = gst_element_get_state(pipeline, &state, NULL, 30 * GST_SECOND);
//     }
//     return ret != GST_STATE_CHANGE_FAILURE ? 1 : 0;
// }
//
// static GstBus *get_bus(GstElement *pipeline) {
//     return gst_element_get_bus(pipeline);
// }
//
// // Sonde le bus GStreamer (timeout 100 ms).
// // Retourne 1=erreur, 2=avertissement, 3=eos, 0=rien.
// static int pop_bus_message(GstBus *bus, char **msg, char **dbg) {
//     *msg = NULL; *dbg = NULL;
//     GstMessage *m = gst_bus_timed_pop_filtered(bus, 100 * GST_MSECOND,
//         GST_MESSAGE_ERROR | GST_MESSAGE_WARNING | GST_MESSAGE_EOS);
//     if (m == NULL) return 0;
//     int ret = 0;
//     GstMessageType t = GST_MESSAGE_TYPE(m);
//     if (t == GST_MESSAGE_ERROR) {
//         ret = 1;
//         GError *err = NULL; gchar *d = NULL;
//         gst_message_parse_error(m, &err, &d);
//         if (err) { *msg = g_strdup(err->message); g_error_free(err); }
//         if (d)   { *dbg = g_strdup(d); g_free(d); }
//     } else if (t == GST_MESSAGE_WARNING) {
//         ret = 2;
//         GError *err = NULL; gchar *d = NULL;
//         gst_message_parse_warning(m, &err, &d);
//         if (err) { *msg = g_strdup(err->message); g_error_free(err); }
//         if (d)   { *dbg = g_strdup(d); g_free(d); }
//     } else if (t == GST_MESSAGE_EOS) {
//         ret = 3;
//     }
//     gst_message_unref(m);
//     return ret;
// }
//
// // Pull une frame depuis l'appsink (timeout 1 s).
// // Retourne GST_FLOW_OK, GST_FLOW_EOS, ou GST_FLOW_CUSTOM_ERROR (timeout).
// static GstFlowReturn pull_sample(GstElement *sink, GstSample **sample) {
//     *sample = gst_app_sink_try_pull_sample(GST_APP_SINK(sink), GST_SECOND);
//     if (*sample == NULL) {
//         if (gst_app_sink_is_eos(GST_APP_SINK(sink))) return GST_FLOW_EOS;
//         return GST_FLOW_CUSTOM_ERROR;
//     }
//     return GST_FLOW_OK;
// }
//
// static GstClockTime buf_duration(GstBuffer *buf) {
//     return GST_BUFFER_DURATION_IS_VALID(buf) ? GST_BUFFER_DURATION(buf) : 0;
// }
//
// static void send_eos(GstElement *pipeline) {
//     gst_element_send_event(pipeline, gst_event_new_eos());
// }
//
// // Vérifie que le plugin SRT GStreamer est disponible en tentant de créer
// // un élément srtsrc. Retourne 1 si OK, 0 si le plugin est absent.
// static int check_srt_plugin(void) {
//     GstElement *e = gst_element_factory_make("srtsrc", NULL);
//     if (!e) return 0;
//     gst_object_unref(e);
//     return 1;
// }
import "C"

import (
	"context"
	"fmt"
	"sync"
	"time"
	"unsafe"

	"racecast-receiver/internal/logger"
)

func init() {
	C.gst_init(nil, nil)
	C.gst_debug_set_active(C.FALSE)
}

// CheckGStreamer vérifie que GStreamer est correctement initialisé et que le
// plugin SRT (srtsrc/srtsink) est disponible. Doit être appelé au démarrage.
func CheckGStreamer() error {
	if C.check_srt_plugin() == 0 {
		return fmt.Errorf(
			"plugin SRT GStreamer introuvable — installez : " +
				"apt install gstreamer1.0-plugins-bad libsrt-gnutls-dev",
		)
	}
	return nil
}

// parseLaunch crée un pipeline GStreamer depuis sa description textuelle.
// Retourne une erreur si la syntaxe est invalide ou si un élément est introuvable.
func parseLaunch(pipelineStr string) (*C.GstElement, error) {
	cStr := C.CString(pipelineStr)
	defer C.free(unsafe.Pointer(cStr))
	var gerr *C.GError
	gp := C.gst_parse_launch(cStr, &gerr)
	if gerr != nil {
		msg := C.GoString((*C.char)(unsafe.Pointer(gerr.message)))
		C.g_error_free(gerr)
		return nil, fmt.Errorf("gst_parse_launch : %s", msg)
	}
	return gp, nil
}

// Frame contient un buffer encodé extrait de l'appsink GStreamer.
type Frame struct {
	Data     []byte
	Duration time.Duration
}

// GstReceiver gère un pipeline GStreamer appsrc→…→appsink.
// Les données brutes (AV1 OBU ou Opus) sont injectées via Push() dans l'appsrc "src".
// Les frames traitées (AV1 temporal units ou Opus) sont lues via Frames().
type GstReceiver struct {
	pipeline *C.GstElement
	appsrc   *C.GstElement
	appsink  *C.GstElement
	frames   chan Frame
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

// newGstReceiver crée un pipeline GStreamer depuis une description.
// Le pipeline doit contenir :
//   - un appsrc nommé "src"
//   - un appsink nommé "sink"
func newGstReceiver(pipelineStr string) (*GstReceiver, error) {
	gp, err := parseLaunch(pipelineStr)
	if err != nil {
		return nil, err
	}

	srcName := C.CString("src")
	defer C.free(unsafe.Pointer(srcName))
	appsrc := C.gst_bin_get_by_name((*C.GstBin)(unsafe.Pointer(gp)), srcName)
	if appsrc == nil {
		C.gst_object_unref(C.gpointer(gp))
		return nil, fmt.Errorf("appsrc 'src' introuvable dans le pipeline")
	}

	sinkName := C.CString("sink")
	defer C.free(unsafe.Pointer(sinkName))
	appsink := C.gst_bin_get_by_name((*C.GstBin)(unsafe.Pointer(gp)), sinkName)
	if appsink == nil {
		C.gst_object_unref(C.gpointer(appsrc))
		C.gst_object_unref(C.gpointer(gp))
		return nil, fmt.Errorf("appsink 'sink' introuvable dans le pipeline")
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &GstReceiver{
		pipeline: gp,
		appsrc:   appsrc,
		appsink:  appsink,
		frames:   make(chan Frame, 16),
		ctx:      ctx,
		cancel:   cancel,
	}, nil
}

// Push injecte un buffer de données dans l'appsrc du pipeline.
func (r *GstReceiver) Push(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	ret := C.push_buffer(r.appsrc, (*C.char)(unsafe.Pointer(&data[0])), C.int(len(data)))
	if ret != C.GST_FLOW_OK {
		return fmt.Errorf("push_buffer : flow=%d", int(ret))
	}
	return nil
}

// EndOfStream signale la fin du flux à l'appsrc ; l'EOS se propage jusqu'à l'appsink.
func (r *GstReceiver) EndOfStream() {
	C.eos_appsrc(r.appsrc)
}

// Start passe le pipeline en PLAYING.
func (r *GstReceiver) Start() error {
	if C.start_pipeline(r.pipeline) == 0 {
		return fmt.Errorf("impossible de démarrer le pipeline GStreamer")
	}
	r.wg.Add(2)
	go func() { defer r.wg.Done(); r.watchBus() }()
	go func() { defer r.wg.Done(); r.loop() }()
	return nil
}

// Frames retourne le canal de frames reçues depuis la Jetson.
func (r *GstReceiver) Frames() <-chan Frame {
	return r.frames
}

// Stop envoie EOS et attend l'arrêt propre (timeout 5 s).
func (r *GstReceiver) Stop() {
	C.send_eos(r.pipeline)
	done := make(chan struct{})
	go func() { r.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		r.cancel()
		r.wg.Wait()
	}
	C.gst_element_set_state(r.pipeline, C.GST_STATE_NULL)
}

// Free libère les ressources GStreamer et désenregistre le receiver.
func (r *GstReceiver) Free() {
	if r.appsrc != nil {
		C.gst_object_unref(C.gpointer(r.appsrc))
	}
	if r.appsink != nil {
		C.gst_object_unref(C.gpointer(r.appsink))
	}
	if r.pipeline != nil {
		C.gst_object_unref(C.gpointer(r.pipeline))
	}
}

// watchBus surveille les messages du bus GStreamer (erreur, avertissement, EOS).
func (r *GstReceiver) watchBus() {
	bus := C.get_bus(r.pipeline)
	if bus == nil {
		return
	}
	defer C.gst_object_unref(C.gpointer(bus))

	for {
		select {
		case <-r.ctx.Done():
			return
		default:
		}
		var cMsg, cDbg *C.char
		ret := C.pop_bus_message(bus, &cMsg, &cDbg)
		msg, dbg := "", ""
		if cMsg != nil {
			msg = C.GoString(cMsg)
			C.g_free(C.gpointer(unsafe.Pointer(cMsg)))
		}
		if cDbg != nil {
			dbg = C.GoString(cDbg)
			C.g_free(C.gpointer(unsafe.Pointer(cDbg)))
		}
		switch ret {
		case 1:
			logger.Error("[gst] Erreur pipeline : %s — %s", msg, dbg)
			r.cancel()
			return
		case 2:
			logger.Warn("[gst] Avertissement : %s — %s", msg, dbg)
		case 3:
			r.cancel()
			return
		}
	}
}

// loop tire les frames de l'appsink et les envoie sur le canal.
func (r *GstReceiver) loop() {
	defer close(r.frames)

	for {
		select {
		case <-r.ctx.Done():
			return
		default:
		}

		var sample *C.GstSample
		flowRet := C.pull_sample(r.appsink, &sample)

		if flowRet == C.GST_FLOW_EOS {
			return
		}
		if flowRet == C.GST_FLOW_CUSTOM_ERROR {
			continue // timeout 1 s, pas de frame disponible
		}

		buf := C.gst_sample_get_buffer(sample)
		if buf == nil {
			C.gst_sample_unref(sample)
			continue
		}

		var mapInfo C.GstMapInfo
		if C.gst_buffer_map(buf, &mapInfo, C.GST_MAP_READ) == C.gboolean(0) {
			C.gst_sample_unref(sample)
			continue
		}

		data := C.GoBytes(unsafe.Pointer(mapInfo.data), C.int(mapInfo.size))
		dur := time.Duration(C.buf_duration(buf))
		if dur <= 0 {
			dur = time.Second / 30
		}

		C.gst_buffer_unmap(buf, &mapInfo)
		C.gst_sample_unref(sample)

		if len(data) == 0 {
			continue
		}

		select {
		case r.frames <- Frame{Data: data, Duration: dur}:
		default: // consommateur trop lent : frame abandonnée
		}
	}
}
