package pipeline

// gst.go gère les pipelines GStreamer de réception SRT + appsink.
//
// Contrainte CGo //export : ce fichier contient des définitions de fonctions C,
// il ne peut donc pas contenir de directives //export. Les fonctions Go exportées
// vers C (GoCallerAdded, GoCallerRemoved) sont dans callbacks.go.

// #cgo pkg-config: gstreamer-1.0 gstreamer-app-1.0
// #cgo LDFLAGS: -ldl
// #include <gst/gst.h>
// #include <gst/app/gstappsink.h>
// #include <dlfcn.h>
// #include <stdint.h>
// #include <stdlib.h>
//
// // Déclarations des callbacks Go exportés (définis dans callbacks.go).
// extern void GoCallerAdded(int64_t id, const char *streamid);
// extern void GoCallerRemoved(int64_t id);
//
// // --- Lecture du streamid SRT via dlopen (pas de headers libsrt requis) ---
// // SRTO_STREAMID = 46 est stable depuis libsrt 1.3.0, y compris la 1.5.x.
// // La bibliothèque est déjà chargée en mémoire par le plugin SRT de GStreamer.
// #define MY_SRTO_STREAMID 46
//
// typedef int (*srt_getsockflag_fn)(int u, int opt, void *optval, int *optlen);
//
// // get_srt_fn() localise srt_getsockflag une seule fois (g_once thread-safe).
// static srt_getsockflag_fn get_srt_fn(void) {
//     static gsize once = 0;
//     static srt_getsockflag_fn fn = NULL;
//     if (g_once_init_enter(&once)) {
//         void *lib = dlopen("libsrt-gnutls.so.1.5", RTLD_LAZY);
//         if (!lib) lib = dlopen("libsrt.so.1.5",        RTLD_LAZY);
//         if (!lib) lib = dlopen("libsrt-gnutls.so.1",   RTLD_LAZY);
//         if (!lib) lib = dlopen("libsrt.so.1",          RTLD_LAZY);
//         if (lib)  fn  = (srt_getsockflag_fn)dlsym(lib, "srt_getsockflag");
//         g_once_init_leave(&once, 1);
//     }
//     return fn;
// }
//
// // Callback GStreamer "caller-added" : lit le streamid et notifie Go.
// static void on_caller_added(GstElement *src, gint sock, gpointer addr,
//                              gpointer user_data) {
//     int64_t id   = (int64_t)(uintptr_t)user_data;
//     char    buf[512] = {0};
//     int     buflen   = 511;
//     srt_getsockflag_fn fn = get_srt_fn();
//     if (fn) fn((int)sock, MY_SRTO_STREAMID, buf, &buflen);
//     GoCallerAdded(id, buf);
// }
//
// // Callback GStreamer "caller-removed" : notifie Go de la déconnexion.
// static void on_caller_removed(GstElement *src, gint sock, gpointer addr,
//                                gpointer user_data) {
//     int64_t id = (int64_t)(uintptr_t)user_data;
//     GoCallerRemoved(id);
// }
//
// // Connecte les signaux caller-added/removed sur l'élément srtsrc nommé "srcsrc".
// static void connect_caller_signals(GstElement *pipeline, int64_t id) {
//     GstElement *src = gst_bin_get_by_name(GST_BIN(pipeline), "srcsrc");
//     if (!src) return;
//     gpointer ud = (gpointer)(uintptr_t)(unsigned long)id;
//     g_signal_connect(src, "caller-added",   G_CALLBACK(on_caller_added),   ud);
//     g_signal_connect(src, "caller-removed", G_CALLBACK(on_caller_removed), ud);
//     gst_object_unref(src);
// }
//
// // Passe le pipeline en PLAYING et attend la fin de l'initialisation asynchrone.
// // En mode listener, PLAYING est atteint dès que le port SRT est lié (pas besoin
// // qu'un caller soit connecté). Retourne 1 si succès, 0 sinon.
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

// Frame contient un buffer encodé extrait de l'appsink GStreamer.
type Frame struct {
	Data     []byte
	Duration time.Duration
}

// GstReceiver gère un pipeline GStreamer de réception SRT avec appsink.
// Chaque instance correspond à un port SRT en écoute (une caméra ou un micro).
// Les métadonnées du flux (nom, source LiveKit) sont transmises par la Jetson
// via le paramètre SRT streamid et récupérées via le signal "caller-added".
type GstReceiver struct {
	mu           sync.Mutex
	pipeline     *C.GstElement
	appsink      *C.GstElement
	frames       chan Frame
	callerEvents chan callerEvent // événements caller-added / caller-removed
	receiverID   int64           // clé dans la table globale recvs
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
	onDisconnect func() // appelé sur EOS ou erreur fatale du pipeline
}

// newGstReceiver crée un pipeline GStreamer depuis une description de pipeline.
// Le pipeline doit contenir :
//   - un appsink nommé "sink"
//   - un srtsrc nommé "srcsrc" (pour connecter les signaux caller-added/removed)
func newGstReceiver(pipelineStr string) (*GstReceiver, error) {
	cStr := C.CString(pipelineStr)
	defer C.free(unsafe.Pointer(cStr))

	var gerr *C.GError
	gp := C.gst_parse_launch(cStr, &gerr)
	if gerr != nil {
		msg := C.GoString((*C.char)(unsafe.Pointer(gerr.message)))
		C.g_error_free(gerr)
		return nil, fmt.Errorf("gst_parse_launch : %s", msg)
	}

	sinkName := C.CString("sink")
	defer C.free(unsafe.Pointer(sinkName))
	appsink := C.gst_bin_get_by_name((*C.GstBin)(unsafe.Pointer(gp)), sinkName)
	if appsink == nil {
		C.gst_object_unref(C.gpointer(gp))
		return nil, fmt.Errorf("appsink 'sink' introuvable dans le pipeline")
	}

	evCh := make(chan callerEvent, 8)
	id := allocReceiverID(evCh)

	ctx, cancel := context.WithCancel(context.Background())
	r := &GstReceiver{
		pipeline:     gp,
		appsink:      appsink,
		frames:       make(chan Frame, 16),
		callerEvents: evCh,
		receiverID:   id,
		ctx:          ctx,
		cancel:       cancel,
	}

	// Connecte les signaux GLib caller-added/removed sur l'élément srtsrc.
	// Quand la Jetson se connecte, on_caller_added lit son streamid via
	// srt_getsockflag et appelle GoCallerAdded (défini dans callbacks.go).
	C.connect_caller_signals(gp, C.int64_t(id))
	return r, nil
}

// SetOnDisconnect enregistre une callback appelée sur EOS ou erreur pipeline.
func (r *GstReceiver) SetOnDisconnect(fn func()) {
	r.mu.Lock()
	r.onDisconnect = fn
	r.mu.Unlock()
}

// CallerEvents retourne le canal des événements de connexion/déconnexion SRT.
func (r *GstReceiver) CallerEvents() <-chan callerEvent {
	return r.callerEvents
}

// Start passe le pipeline en PLAYING. En mode listener SRT, le port est lié
// immédiatement sans attendre de caller. La connexion est notifiée via CallerEvents.
func (r *GstReceiver) Start() error {
	if C.start_pipeline(r.pipeline) == 0 {
		return fmt.Errorf("impossible de démarrer le pipeline GStreamer (SRT listener)")
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
	freeReceiverID(r.receiverID)
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
			logger.Error("[gst] Erreur pipeline (port écoute) : %s — %s", msg, dbg)
			r.notifyDisconnect()
			return
		case 2:
			logger.Warn("[gst] Avertissement : %s — %s", msg, dbg)
		case 3:
			r.notifyDisconnect()
			return
		}
	}
}

func (r *GstReceiver) notifyDisconnect() {
	r.mu.Lock()
	cb := r.onDisconnect
	r.mu.Unlock()
	if cb != nil {
		go cb()
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

// ---------------------------------------------------------------------------
// GstProbe — pipeline léger (fakesink) pour lire le streamid SRT avant de
// créer le vrai pipeline typé (vidéo ou audio).
//
// Flux de fonctionnement :
//  1. GstProbe démarre avec "srtsrc ! fakesink" → lie le port SRT.
//  2. Quand la Jetson se connecte, caller-added fournit le streamid.
//  3. GstProbe retourne le streamid à probeStreamID (receiver.go).
//  4. GstProbe s'arrête → Jetson se déconnecte → reconnecte sur le vrai pipeline.
// ---------------------------------------------------------------------------

// GstProbe gère un pipeline GStreamer minimal (srtsrc → fakesink) utilisé
// uniquement pour lire le streamid SRT avant de créer le pipeline typé.
type GstProbe struct {
	pipeline     *C.GstElement
	callerEvents chan callerEvent
	receiverID   int64
	errCh        chan error
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
}

// newGstProbe crée un pipeline probe depuis une description.
// Le pipeline doit contenir un srtsrc nommé "srcsrc".
func newGstProbe(pipelineStr string) (*GstProbe, error) {
	cStr := C.CString(pipelineStr)
	defer C.free(unsafe.Pointer(cStr))

	var gerr *C.GError
	gp := C.gst_parse_launch(cStr, &gerr)
	if gerr != nil {
		msg := C.GoString((*C.char)(unsafe.Pointer(gerr.message)))
		C.g_error_free(gerr)
		return nil, fmt.Errorf("gst_parse_launch (probe) : %s", msg)
	}

	evCh := make(chan callerEvent, 4)
	id := allocReceiverID(evCh)
	ctx, cancel := context.WithCancel(context.Background())

	p := &GstProbe{
		pipeline:     gp,
		callerEvents: evCh,
		receiverID:   id,
		errCh:        make(chan error, 1),
		ctx:          ctx,
		cancel:       cancel,
	}
	C.connect_caller_signals(gp, C.int64_t(id))
	return p, nil
}

// Start passe le pipeline probe en PLAYING (lie le port SRT sans attendre de caller).
func (p *GstProbe) Start() error {
	if C.start_pipeline(p.pipeline) == 0 {
		return fmt.Errorf("impossible de démarrer le pipeline probe (port SRT déjà utilisé ?)")
	}
	p.wg.Add(1)
	go func() { defer p.wg.Done(); p.watchBus() }()
	return nil
}

// CallerEvents retourne le canal des événements caller-added / caller-removed.
func (p *GstProbe) CallerEvents() <-chan callerEvent { return p.callerEvents }

// ErrCh retourne le canal des erreurs fatales du pipeline probe.
func (p *GstProbe) ErrCh() <-chan error { return p.errCh }

// Stop arrête immédiatement le pipeline probe.
func (p *GstProbe) Stop() {
	p.cancel()
	C.gst_element_set_state(p.pipeline, C.GST_STATE_NULL)
	p.wg.Wait()
}

// Free libère les ressources GStreamer et désenregistre le probe.
func (p *GstProbe) Free() {
	freeReceiverID(p.receiverID)
	if p.pipeline != nil {
		C.gst_object_unref(C.gpointer(p.pipeline))
	}
}

// watchBus surveille le bus du pipeline probe et signale les erreurs fatales.
func (p *GstProbe) watchBus() {
	bus := C.get_bus(p.pipeline)
	if bus == nil {
		return
	}
	defer C.gst_object_unref(C.gpointer(bus))

	for {
		select {
		case <-p.ctx.Done():
			return
		default:
		}
		var cMsg, cDbg *C.char
		ret := C.pop_bus_message(bus, &cMsg, &cDbg)
		msg := ""
		if cMsg != nil {
			msg = C.GoString(cMsg)
			C.g_free(C.gpointer(unsafe.Pointer(cMsg)))
		}
		if cDbg != nil {
			C.g_free(C.gpointer(unsafe.Pointer(cDbg)))
		}
		if ret == 1 {
			select {
			case p.errCh <- fmt.Errorf("erreur pipeline probe : %s", msg):
			default:
			}
			return
		}
	}
}
