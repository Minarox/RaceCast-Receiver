package pipeline

// receiver.go orchestrates the SRT listener and LiveKit publishing.
// One SRT socket on cfg.SRT.Port; each connection's streamid ("name:source")
// is known at handshake. One goroutine per connection: SRT recv → appsrc →
// GStreamer → appsink → LiveKit WriteSample.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
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
// No intermediate queue needed: appsink handles back-pressure with drop=true.
func buildAudioPipeline() string {
	return `appsrc name=src caps="audio/x-opus,rate=48000,channels=2" ` +
		`format=bytes is-live=true ! ` +
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
	logger.Info("[telemetry] IDR request sent for camera %q", name)
}

// streamSlot tracks an active stream goroutine. The Accept loop sends reconnect
// connections into reconn so runStream can resume without destroying the
// LiveKit track. closeNow is closed when the emitter signals an intentional
// stream close, bypassing the reconnect grace period.
type streamSlot struct {
	reconn    chan *SRTConn // buffered(1)
	closeNow  chan struct{}
	closeOnce sync.Once
}

// signalClose closes closeNow exactly once, causing the associated runStream
// goroutine to exit the grace period immediately and unpublish its LiveKit track.
func (sl *streamSlot) signalClose() {
	sl.closeOnce.Do(func() { close(sl.closeNow) })
}

// reconnGraceTimeout is how long runStream keeps the LiveKit track alive and
// waits for the emitter to reconnect after an SRT disconnection.
// Configurable via RC_SRT_RECONNECT_GRACE (seconds, default 5).
func reconnGraceTimeout() time.Duration {
	if s := os.Getenv("RC_SRT_RECONNECT_GRACE"); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v > 0 {
			return time.Duration(v) * time.Second
		}
	}
	return 5 * time.Second
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
	if !first {
		sb.WriteByte(',')
	}
	fmt.Fprintf(&sb, `"updated_at":%d`, time.Now().Unix())
	sb.WriteByte('}')
	return sb.String()
}

// RunStreams starts an SRT listener on cfg.SRT.Port and accepts all incoming
// connections. When the emitter reconnects with the same streamid, the new
// connection is routed to the existing runStream goroutine via a streamSlot
// channel instead of starting a new one.
func RunStreams(ctx context.Context, cfg config.Config, room *lksdk.Room, wg *sync.WaitGroup) {
	listener, err := newSRTListener(cfg.SRT.Port, cfg.SRT.Latency)
	if err != nil {
		logger.Fatal("[srt] Listener port %d: %v", cfg.SRT.Port, err)
	}
	go func() {
		<-ctx.Done()
		listener.Close()
	}()
	logger.Info("[srt] Listener started on port %d (latency=%d ms)", cfg.SRT.Port, cfg.SRT.Latency)

	meta := newRoomMeta()
	registry := newConnRegistry()

	var (
		slotsMu sync.Mutex
		slots   = make(map[string]*streamSlot)
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			conn, streamID, err := listener.Accept()
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				logger.Error("[srt] Accept: %v — retrying...", err)
				continue
			}

			name, source := parseStreamID(streamID)

			if name == "telemetry" {
				logger.Info("[telemetry] Incoming bidirectional connection")
				wg.Add(1)
				go func() {
					defer wg.Done()
					runBiTelemetry(ctx, conn, cfg, meta, registry, func(n string) {
						slotsMu.Lock()
						sl := slots[n]
						slotsMu.Unlock()
						if sl != nil {
							sl.signalClose()
						}
					})
				}()
				continue
			}

			mediaType := mediaTypeFromSource(source)
			logger.Info("[stream:%s] Incoming connection (streamid=%q, type=%s)", name, streamID, mediaType)

			// Route reconnection to the existing goroutine, or start a new one.
			slotsMu.Lock()
			sl := slots[name]
			if sl != nil {
				slotsMu.Unlock()
				select {
				case sl.reconn <- conn:
					logger.Info("[stream:%s] Reconnection queued", name)
				default:
					logger.Warn("[stream:%s] Reconnect channel busy — closing connection", name)
					conn.Close()
				}
				continue
			}
			sl = &streamSlot{reconn: make(chan *SRTConn, 1), closeNow: make(chan struct{})}
			slots[name] = sl
			slotsMu.Unlock()

			wg.Add(1)
			go func(n, s, mt string, sl *streamSlot, c *SRTConn) {
				defer wg.Done()
				defer func() {
					slotsMu.Lock()
					delete(slots, n)
					slotsMu.Unlock()
				}()
				runStream(ctx, c, sl.reconn, sl.closeNow, n, s, mt, room, registry)
			}(name, source, mediaType, sl, conn)
		}
	}()
}

// runStream handles an SRT stream's full lifecycle, including reconnections.
// It publishes one LiveKit track (once) and loops: receive SRT data → push to
// GStreamer → send frames to LiveKit. On SRT disconnection, it waits up to
// reconnGraceTimeout() for the emitter to reconnect before unpublishing.
func runStream(ctx context.Context, firstConn *SRTConn, reconn <-chan *SRTConn, closeNow <-chan struct{},
	name, source, mediaType string, room *lksdk.Room, registry *connRegistry) {

	var pipelineStr string
	switch mediaType {
	case "video":
		pipelineStr = buildVideoPipeline()
	case "audio":
		pipelineStr = buildAudioPipeline()
	default:
		logger.Error("[stream:%s] Unknown media type: %q — closing connection", name, mediaType)
		firstConn.Close()
		return
	}

	// Publish the LiveKit track immediately; it persists across reconnections.
	track, pub, err := publishTrack(name, mediaType, source, room)
	if err != nil {
		logger.Error("[stream:%s] PublishTrack: %v", name, err)
		firstConn.Close()
		return
	}
	defer func() {
		if pub != nil {
			_ = room.LocalParticipant.UnpublishTrack(pub.SID())
			logger.Info("[stream:%s] Track removed from LiveKit", name)
		}
	}()

	grace := reconnGraceTimeout()
	conn := firstConn

streamLoop:
	for {
		// Register before creating the pipeline so IDR requests and stats
		// polling can find the connection from the first moment.
		registry.register(name, conn)

		recv, err := newGstReceiver(pipelineStr)
		if err != nil {
			logger.Error("[stream:%s] Pipeline creation: %v", name, err)
			conn.Close()
			registry.unregister(name)
			return
		}
		if err := recv.Start(); err != nil {
			logger.Error("[stream:%s] Pipeline start: %v", name, err)
			recv.Free()
			conn.Close()
			registry.unregister(name)
			return
		}

		if mediaType == "video" {
			// Forward bitstream warnings as IDR requests to the emitter.
			recv.SetOnDecodeWarning(func() { registry.requestIDR(name) })
			// Request a keyframe immediately so the stream starts on a clean
			// reference picture rather than waiting for the next IDR interval.
			registry.requestIDR(name)
		}

		// SRT → appsrc goroutine. sync.Once prevents double-close when
		// ctx.Done() and a Recv error race against each other.
		recvDone := make(chan struct{})
		var closeOnce sync.Once
		closeConn := func() { closeOnce.Do(func() { conn.Close() }) }

		go func() {
			defer close(recvDone)
			defer closeConn()
			for {
				data, recvErr := conn.Recv()
				if recvErr != nil || data == nil {
					recv.EndOfStream()
					return
				}
				if pushErr := recv.Push(data); pushErr != nil {
					logger.Warn("[stream:%s] Push appsrc: %v", name, pushErr)
					recv.EndOfStream()
					return
				}
			}
		}()

		// Main loop: appsink → LiveKit WriteSample.
		// A 5 s stats ticker logs non-trivial SRT health metrics.
		statsTicker := time.NewTicker(5 * time.Second)
		disconnected := false

	innerLoop:
		for {
			select {
			case <-ctx.Done():
				closeConn() // unblocks the SRT recv goroutine
				<-recvDone
				break innerLoop
			case f, ok := <-recv.Frames():
				if !ok {
					<-recvDone
					disconnected = true
					break innerLoop
				}
				if wErr := track.WriteSample(media.Sample{
					Data:     f.Data,
					Duration: f.Duration,
				}, nil); wErr != nil {
					logger.Warn("[stream:%s] WriteSample: %v", name, wErr)
				}
			case <-statsTicker.C:
				if c := registry.get(name); c != nil {
					loss, rtt, bw := c.Stats()
					if loss > 0.1 || rtt > 50 {
						logger.Info("[stream:%s] SRT stats: loss=%.1f%% rtt=%.0fms bw=%.2fMbps",
							name, loss, rtt, bw)
					}
				}
			}
		}

		statsTicker.Stop()
		registry.unregister(name)
		recv.Stop()
		recv.Free()

		if !disconnected {
			// ctx cancelled — clean exit, track will be deferred-unpublished.
			break streamLoop
		}

		// Connection dropped — keep the track alive and wait for a reconnect.
		logger.Info("[stream:%s] Connection lost — waiting %.0fs for reconnection...",
			name, grace.Seconds())
		select {
		case newConn := <-reconn:
			logger.Info("[stream:%s] Reconnected — resuming stream", name)
			conn = newConn
			// Continue streamLoop: recreate the GStreamer pipeline for the new connection.
		case <-closeNow:
			logger.Info("[stream:%s] Closed by emitter — unpublishing track immediately", name)
			break streamLoop
		case <-time.After(grace):
			logger.Info("[stream:%s] Grace period expired — unpublishing track", name)
			break streamLoop
		case <-ctx.Done():
			break streamLoop
		}
	}
}

// runBiTelemetry handles the bidirectional SRT telemetry connection.
// Incoming: typed envelopes routed by "type" ("ups"/"modem" → LiveKit metadata).
// Flush: leading-edge throttle, 1 s cooldown.
// Outgoing: IDR requests via registry.requestIDR on GstReceiver decode-warning.
func runBiTelemetry(ctx context.Context, conn *SRTConn, cfg config.Config, meta *roomMeta,
	registry *connRegistry, signalClose func(name string)) {
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
			Type   string          `json:"type"`
			Stream string          `json:"stream,omitempty"`
			Data   json.RawMessage `json:"data,omitempty"`
		}
		if err := json.Unmarshal(data, &envelope); err != nil || envelope.Type == "" {
			logger.Warn("[telemetry] Unrecognised message ignored")
			continue
		}

		switch envelope.Type {
		case "ups", "modem":
			meta.update(envelope.Type, envelope.Data)
		case "stream_close":
			if envelope.Stream != "" {
				logger.Info("[telemetry] Stream close signal received for %q", envelope.Stream)
				signalClose(envelope.Stream)
			}
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
