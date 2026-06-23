package pipeline

// receiver.go orchestrates the SRT listener and LiveKit publishing.
// One SRT socket on cfg.SRT.Port; each connection's streamid ("name:source")
// is known at handshake. One goroutine per connection: SRT recv → appsrc →
// GStreamer → appsink → LiveKit WriteSample.

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	lkproto "github.com/livekit/protocol/livekit"
	lksdk "github.com/livekit/server-sdk-go/v2"
	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"

	"racecast-receiver/internal/config"
	"racecast-receiver/internal/logger"
)

// buildVideoPipeline decodes a raw AV1 OBU stream injected via appsrc.
// av1parse aligns on temporal units (alignment=tu): one buffer = one AV1 frame.
func buildVideoPipeline() string {
	return `appsrc name=src caps="video/x-av1,stream-format=obu-stream" ` +
		`format=bytes is-live=true ! ` +
		`av1parse ! video/x-av1,stream-format=obu-stream,alignment=tu ! ` +
		`appsink name=sink max-buffers=4 drop=true sync=false`
}

// buildAudioPipeline receives raw Opus frames injected via appsrc.
// SRT is message-oriented: each srt_recvmsg = one buffer = one Opus frame.
func buildAudioPipeline() string {
	return `appsrc name=src caps="audio/x-opus,rate=48000,channels=2" ` +
		`format=bytes is-live=true ! ` +
		`queue max-size-buffers=16 leaky=downstream ! ` +
		`appsink name=sink max-buffers=16 drop=true sync=false`
}

// roomMeta accumulates telemetry data from multiple streams into one JSON object
// pushed to LiveKit. snapshot() drains dirty state; notify is signalled on each update.
type roomMeta struct {
	mu     sync.Mutex
	fields map[string]json.RawMessage
	dirty  bool
	notify chan struct{} // buffered(1): at most one pending flush signal
}

func newRoomMeta() *roomMeta {
	return &roomMeta{notify: make(chan struct{}, 1)}
}

// connRegistry tracks active SRT video connections and the shared bidirectional
// telemetry connection for IDR requests.
type connRegistry struct {
	mu        sync.RWMutex
	conns     map[string]*SRTConn // camera name → active SRT video connection
	telemMu   sync.Mutex
	telemConn *SRTConn            // shared bidirectional telemetry connection
}

func newConnRegistry() *connRegistry {
	return &connRegistry{
		conns: make(map[string]*SRTConn),
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

// setTelemConn stores the active bidirectional telemetry connection.
func (r *connRegistry) setTelemConn(conn *SRTConn) {
	r.telemMu.Lock()
	r.telemConn = conn
	r.telemMu.Unlock()
}

// clearTelemConn removes the telemetry connection reference when it closes.
func (r *connRegistry) clearTelemConn() {
	r.telemMu.Lock()
	r.telemConn = nil
	r.telemMu.Unlock()
}

// requestIDR sends an IDR request to the emitter via the telemetry connection.
// Called from the GstReceiver decode-warning callback.
func (r *connRegistry) requestIDR(name string) {
	r.telemMu.Lock()
	conn := r.telemConn
	r.telemMu.Unlock()
	if conn == nil {
		return
	}
	data, _ := json.Marshal(map[string]string{"type": "idr", "camera": name})
	if err := conn.Send(data); err != nil {
		logger.Warn("[telemetry] IDR request for %q: %v", name, err)
		return
	}
	logger.Info("[telemetry] IDR request sent for camera %q (decode warning)", name)
}

// update stores data for the given key and signals the flush goroutine.
	// Non-blocking: if a flush is already pending, data is picked up on next snapshot().
func (m *roomMeta) update(key string, data json.RawMessage) {
	m.mu.Lock()
	if m.fields == nil {
		m.fields = make(map[string]json.RawMessage)
	}
	m.fields[key] = data
	m.dirty = true
	m.mu.Unlock()
	select {
	case m.notify <- struct{}{}:
	default: // a flush is already scheduled; data will be picked up by snapshot()
	}
}

// snapshot returns the merged JSON string and resets the dirty flag.
// Returns ("", false) when no update has occurred since the last snapshot.
func (m *roomMeta) snapshot() (string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.dirty {
		return "", false
	}
	m.dirty = false
	return m.buildJSON(), true
}

// buildJSON serialises m.fields to a JSON object. mu must be held.
func (m *roomMeta) buildJSON() string {
	// Build merged JSON preserving order: ups, modem, then others (sorted).
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
	// Collect and sort any remaining keys for deterministic JSON output.
	var extra []string
	for k := range m.fields {
		if !seen[k] {
			extra = append(extra, k)
		}
	}
	sort.Strings(extra)
	for _, k := range extra {
		if !first {
			sb.WriteByte(',')
		}
		fmt.Fprintf(&sb, "%q:%s", k, m.fields[k])
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
	meta := newRoomMeta()

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
			if name == "telemetry" {
				logger.Info("[telemetry] Incoming bidirectional connection")
				go func() {
					defer wg.Done()
					runBiTelemetry(ctx, conn, cfg, meta, registry)
				}()
			} else {
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

// runStream handles a complete SRT connection lifecycle:
// create GStreamer pipeline, publish LiveKit track, relay SRT → appsrc → LiveKit.
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

// runBiTelemetry handles the bidirectional SRT telemetry connection.
// Incoming: typed envelopes routed by "type" ("ups"/"modem" → LiveKit metadata).
// Flush: leading-edge throttle, 1 s cooldown.
// Outgoing: IDR requests via registry.requestIDR on GstReceiver decode-warning.
func runBiTelemetry(ctx context.Context, conn *SRTConn, cfg config.Config, meta *roomMeta, registry *connRegistry) {
	registry.setTelemConn(conn)
	defer registry.clearTelemConn()
	defer conn.Close()

	client := lksdk.NewRoomServiceClient(cfg.LiveKit.APIURL(), cfg.LiveKit.APIKey, cfg.LiveKit.APISecret)
	logger.Info("[telemetry] Bidirectional connection established")

	// flush pushes the current aggregated state to LiveKit.
	flush := func() {
		merged, ok := meta.snapshot()
		if !ok {
			return
		}
		if _, err := client.UpdateRoomMetadata(ctx, &lkproto.UpdateRoomMetadataRequest{
			Room:     cfg.LiveKit.Room,
			Metadata: merged,
		}); err != nil && ctx.Err() == nil {
			logger.Warn("[telemetry] UpdateRoom: %v", err)
		}
	}

	// Flush goroutine: leading-edge throttle, 1 s cooldown between LiveKit pushes.
	const cooldown = time.Second
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-meta.notify:
				flush()
				// Cooldown: accumulate data for up to 1 s before the next push.
				select {
				case <-ctx.Done():
					return
				case <-time.After(cooldown):
				}
			}
		}
	}()

	for {
		data, err := conn.Recv()
		if err != nil || data == nil {
			logger.Info("[telemetry] Connection closed")
			return
		}

		var envelope struct {
			Type string          `json:"type"`
			Data json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal(data, &envelope); err != nil || envelope.Type == "" {
			logger.Warn("[telemetry] Unrecognised message ignored")
			continue
		}

		switch envelope.Type {
		case "ups", "modem":
			meta.update(envelope.Type, envelope.Data)
		default:
			logger.Warn("[telemetry] Unknown message type %q", envelope.Type)
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
