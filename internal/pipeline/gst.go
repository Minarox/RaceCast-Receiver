package pipeline

// gst.go manages GStreamer appsrc→…→appsink pipelines used to decode
// AV1 and Opus streams received via libsrt (listener.go).
//
// Data arrives via Push() into the "src" appsrc.
// Decoded frames are read by the consumer via Frames().

// #cgo pkg-config: gstreamer-1.0 gstreamer-app-1.0
// #include <gst/gst.h>
// #include <gst/app/gstappsink.h>
// #include <gst/app/gstappsrc.h>
// #include <stdlib.h>
//
// // push_buffer copies data into a GstBuffer and pushes it into appsrc.
// // gst_app_src_push_buffer takes ownership of the buffer (no free on Go side).
// static GstFlowReturn push_buffer(GstElement *src, const char *data, int len) {
//     GstBuffer *buf = gst_buffer_new_allocate(NULL, (gsize)len, NULL);
//     GstMapInfo m;
//     gst_buffer_map(buf, &m, GST_MAP_WRITE);
//     memcpy(m.data, data, (size_t)len);
//     gst_buffer_unmap(buf, &m);
//     return gst_app_src_push_buffer(GST_APP_SRC(src), buf);
// }
//
// // eos_appsrc sends an EOS event into appsrc; it propagates to appsink.
// static void eos_appsrc(GstElement *src) {
//     gst_app_src_end_of_stream(GST_APP_SRC(src));
// }
//
// // Sets the pipeline to PLAYING and waits for async state change to complete.
// // Returns 1 on success, 0 on failure.
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
// // Poll the GStreamer bus (100 ms timeout).
// // Returns 1=error, 2=warning, 3=eos, 0=nothing.
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
// // Pull a sample from appsink (1 s timeout).
// // Returns GST_FLOW_OK, GST_FLOW_EOS, or GST_FLOW_CUSTOM_ERROR (timeout).
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
// // Check that the SRT GStreamer plugin is available by trying to create
// // an srtsrc element. Returns 1 if OK, 0 if missing.
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

// CheckGStreamer verifies that GStreamer is properly initialized and the SRT plugin
// (srtsrc/srtsink) is available. Must be called at startup.
func CheckGStreamer() error {
	if C.check_srt_plugin() == 0 {
		return fmt.Errorf(
			"SRT GStreamer plugin not found — install: " +
				"apt install gstreamer1.0-plugins-bad libsrt-gnutls-dev",
		)
	}
	return nil
}

// parseLaunch creates a GStreamer pipeline from its textual description.
func parseLaunch(pipelineStr string) (*C.GstElement, error) {
	cStr := C.CString(pipelineStr)
	defer C.free(unsafe.Pointer(cStr))
	var gerr *C.GError
	gp := C.gst_parse_launch(cStr, &gerr)
	if gerr != nil {
		msg := C.GoString((*C.char)(unsafe.Pointer(gerr.message)))
		C.g_error_free(gerr)
		return nil, fmt.Errorf("gst_parse_launch: %s", msg)
	}
	return gp, nil
}

// Frame holds an encoded buffer extracted from the GStreamer appsink.
type Frame struct {
	Data     []byte
	Duration time.Duration
}

// GstReceiver manages a GStreamer appsrc→…→appsink pipeline.
// Raw data (AV1 OBU or Opus) is injected via Push() into the "src" appsrc.
// Processed frames are read via Frames().
type GstReceiver struct {
	mu              sync.Mutex // protects onDecodeWarning
	pipeline        *C.GstElement
	appsrc          *C.GstElement
	appsink         *C.GstElement
	frames          chan Frame
	ctx             context.Context
	cancel          context.CancelFunc
	wg              sync.WaitGroup
	onDecodeWarning func()
}

// newGstReceiver creates a GStreamer pipeline from a description.
// The pipeline must contain an appsrc named "src" and an appsink named "sink".
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
		return nil, fmt.Errorf("appsrc 'src' not found in pipeline")
	}

	sinkName := C.CString("sink")
	defer C.free(unsafe.Pointer(sinkName))
	appsink := C.gst_bin_get_by_name((*C.GstBin)(unsafe.Pointer(gp)), sinkName)
	if appsink == nil {
		C.gst_object_unref(C.gpointer(appsrc))
		C.gst_object_unref(C.gpointer(gp))
		return nil, fmt.Errorf("appsink 'sink' not found in pipeline")
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

// SetOnDecodeWarning registers a callback invoked when the GStreamer pipeline
// emits a non-fatal warning (e.g., corrupted AV1 bitstream). The callback is
// called in a separate goroutine and used to trigger IDR requests.
func (r *GstReceiver) SetOnDecodeWarning(fn func()) {
	r.mu.Lock()
	r.onDecodeWarning = fn
	r.mu.Unlock()
}

// Push injects a data buffer into the pipeline's appsrc.
func (r *GstReceiver) Push(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	ret := C.push_buffer(r.appsrc, (*C.char)(unsafe.Pointer(&data[0])), C.int(len(data)))
	if ret != C.GST_FLOW_OK {
		return fmt.Errorf("push_buffer: flow=%d", int(ret))
	}
	return nil
}

// EndOfStream signals end-of-stream to appsrc; the EOS propagates to appsink.
func (r *GstReceiver) EndOfStream() {
	C.eos_appsrc(r.appsrc)
}

// Start sets the pipeline to PLAYING.
func (r *GstReceiver) Start() error {
	if C.start_pipeline(r.pipeline) == 0 {
		return fmt.Errorf("failed to start GStreamer pipeline")
	}
	r.wg.Add(2)
	go func() { defer r.wg.Done(); r.watchBus() }()
	go func() { defer r.wg.Done(); r.loop() }()
	return nil
}

// Frames returns the channel of frames received from the Jetson.
func (r *GstReceiver) Frames() <-chan Frame {
	return r.frames
}

// Stop sends EOS and waits for clean shutdown (5 s timeout).
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

// Free releases GStreamer resources and unregisters the receiver.
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

// watchBus monitors GStreamer bus messages (error, warning, EOS).
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
			logger.Error("[gst] Pipeline error: %s — %s", msg, dbg)
			r.cancel()
			return
		case 2:
			logger.Warn("[gst] Warning: %s — %s", msg, dbg)
			r.mu.Lock()
			cb := r.onDecodeWarning
			r.mu.Unlock()
			if cb != nil {
				go cb()
			}
		case 3:
			r.cancel()
			return
		}
	}
}

// loop pulls frames from appsink and sends them to the channel.
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
			continue // 1 s timeout, no frame available
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
		default: // consumer too slow: frame dropped
		}
	}
}
