package pipeline

// receiver.go orchestrates the single SRT listener and LiveKit publishing.
//
// Architecture:
//   - One SRT socket listening on cfg.SRT.Port.
//   - Each incoming connection returns its streamid ("name:source") during
//     the SRT handshake — the stream type is known immediately.
//   - One goroutine per connection: SRT recv → appsrc → GStreamer → appsink
//     → LiveKit WriteSample.
//   - The Jetson sends all streams in caller mode on the same port;
//     discrimination is done solely by streamid.

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	lkproto "github.com/livekit/protocol/livekit"
	lksdk "github.com/livekit/server-sdk-go/v2"
	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"

	"racecast-receiver/internal/config"
	"racecast-receiver/internal/logger"
)

// buildVideoPipeline builds the GStreamer pipeline to decode raw AV1 OBU.
//
// The Jetson sends raw AV1 OBU stream (nvv4l2av1enc → av1parse → srtsink).
// Bytes are injected into appsrc; av1parse re-syncs and aligns on temporal
// units (alignment=tu) — each appsink buffer = one complete AV1 frame.
func buildVideoPipeline() string {
	return `appsrc name=src caps="video/x-av1,stream-format=obu-stream" ` +
		`format=bytes is-live=true ! ` +
		`av1parse ! video/x-av1,stream-format=obu-stream,alignment=tu ! ` +
		`appsink name=sink max-buffers=4 drop=true sync=false`
}

// buildAudioPipeline builds the GStreamer pipeline to receive raw Opus.
//
// The Jetson sends raw Opus frames (opusenc → srtsink).
// SRT is message-oriented: each srt_recvmsg = one GStreamer buffer = one Opus frame.
func buildAudioPipeline() string {
	return `appsrc name=src caps="audio/x-opus,rate=48000,channels=2" ` +
		`format=bytes is-live=true ! ` +
		`queue max-size-buffers=16 leaky=downstream ! ` +
		`appsink name=sink max-buffers=16 drop=true sync=false`
}

// roomMeta keeps the latest data from each telemetry stream.
// It merges ups and modem into a single JSON object sent to LiveKit.
type roomMeta struct {
	mu    sync.Mutex
	fields map[string]json.RawMessage
}

// feedbackMsg is the JSON signal sent from the receiver to the emitter when
// the AV1 decoder pipeline emits a non-fatal bitstream warning.
// The emitter reacts by forcing an immediate IDR frame on the encoder.
type feedbackMsg struct {
	RequestIDR bool `json:"request_idr"`
}

// connRegistry tracks active SRT video connections and the IDR signal channels
// of their associated feedback goroutines.
type connRegistry struct {
	mu     sync.RWMutex
	conns  map[string]*SRTConn      // camera name → active SRT video connection
	idrChs map[string]chan struct{} // camera name → feedback goroutine signal channel
}

func newConnRegistry() *connRegistry {
	return &connRegistry{
		conns:  make(map[string]*SRTConn),
		idrChs: make(map[string]chan struct{}),
	}
}

func (r *connRegistry) register(name string, conn *SRTConn) {
	r.mu.Lock()
	r.conns[name] = conn
	r.mu.Unlock()
}

func (r *connRegistry) unregister(name string) {
	r.mu.Lock()
	delete(r.conns, name)
	r.mu.Unlock()
}

func (r *connRegistry) get(name string) *SRTConn {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.conns[name]
}

// setIDRCh registers the channel used by the feedback goroutine for name.
func (r *connRegistry) setIDRCh(name string, ch chan struct{}) {
	r.mu.Lock()
	r.idrChs[name] = ch
	r.mu.Unlock()
}

// removeIDRCh deregisters the IDR channel when the feedback connection closes.
func (r *connRegistry) removeIDRCh(name string) {
	r.mu.Lock()
	delete(r.idrChs, name)
	r.mu.Unlock()
}

// requestIDR sends a non-blocking IDR signal to the feedback goroutine.
// Called from the GstReceiver decode-warning callback.
func (r *connRegistry) requestIDR(name string) {
	r.mu.RLock()
	ch := r.idrChs[name]
	r.mu.RUnlock()
	if ch != nil {
		select {
		case ch <- struct{}{}:
		default: // already pending — ignore duplicate
		}
	}
}

// update stores data for the given key and returns the merged JSON.
func (m *roomMeta) update(key string, data json.RawMessage) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.fields == nil {
		m.fields = make(map[string]json.RawMessage)
	}
	m.fields[key] = data
	// Build merged JSON preserving order: ups, modem, then others.
	order := []string{"ups", "modem"}
	seen := make(map[string]bool, len(order))
	var sb strings.Builder
	sb.WriteByte('{')
	first := true
	for _, k := range order {
		if v, ok := m.fields[k]; ok {
			if !first {
				sb.WriteByte(',')
			}
			fmt.Fprintf(&sb, "%q:%s", k, v)
			first = false
			seen[k] = true
		}
	}
	for k, v := range m.fields {
		if seen[k] {
			continue
		}
		if !first {
			sb.WriteByte(',')
		}
		fmt.Fprintf(&sb, "%q:%s", k, v)
		first = false
	}
	sb.WriteByte('}')
	return sb.String()
}

// RunStreams starts an SRT listener on cfg.SRT.Port and accepts all incoming
// connections, each handled in its own goroutine.
func RunStreams(ctx context.Context, cfg config.Config, room *lksdk.Room, wg *sync.WaitGroup) {
	listener, err := newSRTListener(cfg.SRT.Port, cfg.SRT.Latency)
	if err != nil {
		logger.Fatal("[srt] Listener port %d: %v", cfg.SRT.Port, err)
	}

	// Close the listener when ctx is cancelled (unblocks Accept).
	go func() {
		<-ctx.Done()
		listener.Close()
	}()

	logger.Info("[srt] Listener started on port %d (latency=%d ms)", cfg.SRT.Port, cfg.SRT.Latency)

	// Shared state to merge telemetry streams into a single JSON object.
	meta := &roomMeta{}

	// Registry of active SRT video connections indexed by camera name.
	// Used by feedback goroutines to read per-stream SRT statistics.
	registry := newConnRegistry()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			conn, streamID, err := listener.Accept()
			if err != nil {
				if ctx.Err() != nil {
					return // clean shutdown
				}
				logger.Error("[srt] Accept: %v — retrying...", err)
				continue
			}
			name, source := parseStreamID(streamID)
			wg.Add(1)
			switch source {
			case "ups", "modem":
				logger.Info("[telemetry:%s] Incoming connection", source)
				go func(src string) {
					defer wg.Done()
					runTelemetry(ctx, conn, src, cfg, meta)
				}(source)
			case "feedback":
				logger.Info("[feedback:%s] Incoming feedback connection", name)
				go func(n string) {
					defer wg.Done()
					runFeedback(ctx, conn, n, registry)
				}(name)
			default:
				mediaType := mediaTypeFromSource(source)
				logger.Info("[stream:%s] Incoming connection (streamid=%q, type=%s)", name, streamID, mediaType)
				go func(n, s, mt string) {
					defer wg.Done()
					runStream(ctx, conn, n, s, mt, room, registry)
				}(name, source, mediaType)
			}
		}
	}()
}

// runStream handles a complete lifecycle for an accepted SRT connection:
//  1. Create and start the GStreamer pipeline (appsrc → … → appsink).
//  2. Publish the LiveKit track immediately (type known from streamid).
//  3. SRT goroutine: recv → Push() into appsrc.
//  4. Main loop: Frames() → LiveKit WriteSample.
//  5. On connection close or EOS: unpublish, Stop, Free.
func runStream(ctx context.Context, conn *SRTConn, name, source, mediaType string, room *lksdk.Room, registry *connRegistry) {
	var pipelineStr string
	switch mediaType {
	case "video":
		pipelineStr = buildVideoPipeline()
	case "audio":
		pipelineStr = buildAudioPipeline()
	default:
		logger.Error("[stream:%s] Unknown media type: %q — closing connection", name, mediaType)
		conn.Close()
		return
	}

	recv, err := newGstReceiver(pipelineStr)
	if err != nil {
		logger.Error("[stream:%s] Pipeline creation: %v", name, err)
		conn.Close()
		return
	}
	if err := recv.Start(); err != nil {
		logger.Error("[stream:%s] Pipeline start: %v", name, err)
		recv.Free()
		conn.Close()
		return
	}

	track, pub, err := publishTrack(name, mediaType, source, room)
	if err != nil {
		logger.Error("[stream:%s] PublishTrack: %v", name, err)
		recv.Stop()
		recv.Free()
		conn.Close()
		return
	}

	// Register connection so the feedback goroutine can poll SRT stats.
	registry.register(name, conn)
	defer registry.unregister(name)

	// Forward GStreamer AV1 decode warnings as IDR requests to the emitter.
	if mediaType == "video" {
		recv.SetOnDecodeWarning(func() { registry.requestIDR(name) })
	}

	// SRT → appsrc goroutine. Owns conn and closes it when done.
	recvDone := make(chan struct{})
	go func() {
		defer close(recvDone)
		defer conn.Close()
		for {
			data, recvErr := conn.Recv()
			if recvErr != nil || data == nil {
				recv.EndOfStream()
				return
			}
			if pushErr := recv.Push(data); pushErr != nil {
				logger.Warn("[stream:%s] Push appsrc : %v", name, pushErr)
				recv.EndOfStream()
				return
			}
		}
	}()

	// Main loop: appsink → LiveKit WriteSample.
loop:
	for {
		select {
		case <-ctx.Done():
			conn.Close()  // unblocks the SRT receive goroutine
			<-recvDone
			break loop
		case f, ok := <-recv.Frames():
			if !ok {
				<-recvDone
				break loop
			}
			if wErr := track.WriteSample(media.Sample{
				Data:     f.Data,
				Duration: f.Duration,
			}, nil); wErr != nil {
				logger.Warn("[stream:%s] WriteSample : %v", name, wErr)
			}
		}
	}

	if pub != nil {
		_ = room.LocalParticipant.UnpublishTrack(pub.SID())
		logger.Info("[stream:%s] Track removed from LiveKit", name)
	}
	recv.Stop()
	recv.Free()
}

// runTelemetry reads JSON messages from a telemetry SRT connection and updates
// LiveKit room metadata on each message. key identifies the stream ("ups", "modem", ...);
// data is merged into meta before sending so streams don't overwrite each other.
func runTelemetry(ctx context.Context, conn *SRTConn, key string, cfg config.Config, meta *roomMeta) {
	defer conn.Close()
	client := lksdk.NewRoomServiceClient(cfg.LiveKit.APIURL(), cfg.LiveKit.APIKey, cfg.LiveKit.APISecret)

	for {
		data, err := conn.Recv()
		if err != nil || data == nil {
			logger.Info("[telemetry:%s] Connection closed", key)
			return
		}

		if !json.Valid(data) {
			logger.Warn("[telemetry:%s] Non-JSON message ignored", key)
			continue
		}

		merged := meta.update(key, json.RawMessage(data))

		if _, err := client.UpdateRoomMetadata(ctx, &lkproto.UpdateRoomMetadataRequest{
			Room:     cfg.LiveKit.Room,
			Metadata: merged,
		}); err != nil && ctx.Err() == nil {
			logger.Warn("[telemetry:%s] UpdateRoom : %v", key, err)
		}
	}
}

// parseStreamID extracts name and source from the SRT streamid.
// Format: "name:source" (e.g. "Route:camera"). Returns defaults on empty/invalid input.
func parseStreamID(streamID string) (name, source string) {
	parts := strings.SplitN(streamID, ":", 2)
	name = strings.TrimSpace(parts[0])
	if name == "" {
		name = "stream"
	}
	if len(parts) > 1 {
		source = strings.TrimSpace(parts[1])
	}
	if source == "" {
		source = "camera"
	}
	return
}

// mediaTypeFromSource infers the media type from the LiveKit source string.
// "microphone" → "audio", anything else → "video".
func mediaTypeFromSource(source string) string {
	if source == "microphone" {
		return "audio"
	}
	return "video"
}

// publishTrack creates and publishes a LiveKit track based on the media type.
func publishTrack(name, mediaType, source string, room *lksdk.Room) (*lksdk.LocalSampleTrack, *lksdk.LocalTrackPublication, error) {
	var codec webrtc.RTPCodecCapability
	var opts lksdk.TrackPublicationOptions

	switch mediaType {
	case "video":
		codec = webrtc.RTPCodecCapability{
			MimeType:  webrtc.MimeTypeAV1,
			ClockRate: 90000,
		}
		opts = lksdk.TrackPublicationOptions{
			Name:   name,
			Source: trackSource(source),
		}
	case "audio":
		codec = webrtc.RTPCodecCapability{
			MimeType:  webrtc.MimeTypeOpus,
			ClockRate: 48000,
			Channels:  2,
		}
		opts = lksdk.TrackPublicationOptions{
			Name:   name,
			Source: trackSource(source),
			Stereo: true,
		}
	default:
		return nil, nil, fmt.Errorf("unknown media type: %q", mediaType)
	}

	track, err := lksdk.NewLocalSampleTrack(codec)
	if err != nil {
		return nil, nil, fmt.Errorf("NewLocalSampleTrack : %w", err)
	}

	pub, err := room.LocalParticipant.PublishTrack(track, &opts)
	if err != nil {
		return nil, nil, fmt.Errorf("PublishTrack : %w", err)
	}

	logger.Info("[stream:%s] %s track published in LiveKit", name, mediaType)
	return track, pub, nil
}

// trackSource converts a source string to a LiveKit proto constant.
func trackSource(s string) lkproto.TrackSource {
	switch s {
	case "microphone":
		return lkproto.TrackSource_MICROPHONE
	default:
		return lkproto.TrackSource_CAMERA
	}
}

// runFeedback listens for IDR request signals from the emitter's receiver-side
// decode-warning callback. It is event-driven: a message is only sent when the
// GstReceiver pipeline emits a non-fatal AV1 bitstream warning, so no
// periodic polling or network stats round-trip is needed.
func runFeedback(ctx context.Context, conn *SRTConn, cameraName string, registry *connRegistry) {
	defer conn.Close()

	// Create a buffered channel (capacity 1) so duplicate rapid warnings coalesce.
	idrCh := make(chan struct{}, 1)
	registry.setIDRCh(cameraName, idrCh)
	defer registry.removeIDRCh(cameraName)

	logger.Info("[feedback:%s] IDR request listener active", cameraName)

	for {
		select {
		case <-ctx.Done():
			return
		case <-idrCh:
			msg := feedbackMsg{RequestIDR: true}
			data, _ := json.Marshal(msg)
			if err := conn.Send(data); err != nil {
				logger.Warn("[feedback:%s] Send failed: %v", cameraName, err)
				return
			}
			logger.Info("[feedback:%s] IDR request sent to emitter (decode warning)", cameraName)
		}
	}
}
