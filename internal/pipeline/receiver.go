package pipeline

// receiver.go orchestre le listener SRT unique et la publication dans LiveKit.
//
// Architecture port unique :
//   - Un seul socket SRT en écoute sur cfg.SRT.Port.
//   - Chaque connexion entrante retourne son streamid ("name:source") pendant
//     le handshake SRT — le type de flux est connu immédiatement.
//   - Goroutine par connexion : SRT recv → appsrc → pipeline GStreamer → appsink
//     → WriteSample LiveKit. Pas de phase probe, pas de reconnexion attendue.
//   - La Jetson envoie ses streams en mode caller sur le même port ; la
//     discrimination se fait uniquement par streamid.

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

// buildVideoPipeline construit le pipeline GStreamer pour décoder de l'AV1 OBU.
//
// La Jetson envoie de l'AV1 OBU stream brut (nvv4l2av1enc → av1parse → srtsink).
// On injecte ces octets dans appsrc ; av1parse resynchronise et aligne sur les
// temporal units (alignment=tu) — chaque buffer appsink = une trame AV1 complète.
func buildVideoPipeline() string {
	return `appsrc name=src caps="video/x-av1,stream-format=obu-stream" ` +
		`format=bytes is-live=true ! ` +
		`av1parse ! video/x-av1,stream-format=obu-stream,alignment=tu ! ` +
		`appsink name=sink max-buffers=4 drop=true sync=false`
}

// buildAudioPipeline construit le pipeline GStreamer pour recevoir de l'Opus.
//
// La Jetson envoie des trames Opus brutes (opusenc → srtsink).
// SRT étant orienté message, chaque srt_recvmsg = un buffer GStreamer = une trame Opus.
func buildAudioPipeline() string {
	return `appsrc name=src caps="audio/x-opus,rate=48000,channels=2" ` +
		`format=bytes is-live=true ! ` +
		`queue max-size-buffers=16 leaky=downstream ! ` +
		`appsink name=sink max-buffers=16 drop=true sync=false`
}

// roomMeta conserve les dernières données de chaque flux de télémétrie.
// Elle fusionne ups et modem en un seul objet JSON envoyé à LiveKit.
type roomMeta struct {
	mu    sync.Mutex
	fields map[string]json.RawMessage
}

// update stocke les données pour la clé donnée et retourne le JSON fusionné.
func (m *roomMeta) update(key string, data json.RawMessage) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.fields == nil {
		m.fields = make(map[string]json.RawMessage)
	}
	m.fields[key] = data
	// Construire le JSON fusionné en préservant l'ordre : ups, modem, puis autres.
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

// RunStreams démarre un listener SRT sur cfg.SRT.Port et accepte toutes les
// connexions entrantes. Chaque connexion est gérée dans sa propre goroutine.
func RunStreams(ctx context.Context, cfg config.Config, room *lksdk.Room, wg *sync.WaitGroup) {
	listener, err := newSRTListener(cfg.SRT.Port, cfg.SRT.Latency)
	if err != nil {
		logger.Fatal("[srt] Listener port %d : %v", cfg.SRT.Port, err)
	}

	// Fermer le listener quand le contexte est annulé (débloque Accept).
	go func() {
		<-ctx.Done()
		listener.Close()
	}()

	logger.Info("[srt] Listener démarré sur le port %d (latency=%d ms)", cfg.SRT.Port, cfg.SRT.Latency)

	// État partagé pour fusionner les télémétries (ups, modem…) en un seul JSON.
	meta := &roomMeta{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			conn, streamID, err := listener.Accept()
			if err != nil {
				if ctx.Err() != nil {
					return // arrêt normal
				}
				logger.Error("[srt] Accept : %v — nouvelle tentative...", err)
				continue
			}
			name, source := parseStreamID(streamID)
			wg.Add(1)
			if source == "ups" || source == "modem" {
				logger.Info("[telemetry:%s] Connexion entrante", source)
				go func(src string) {
					defer wg.Done()
					runTelemetry(ctx, conn, src, cfg, meta)
				}(source)
			} else {
				mediaType := mediaTypeFromSource(source)
				logger.Info("[stream:%s] Connexion entrante (streamid=%q, type=%s)", name, streamID, mediaType)
				go func(n, s, mt string) {
					defer wg.Done()
					runStream(ctx, conn, n, s, mt, room)
				}(name, source, mediaType)
			}
		}
	}()
}

// runStream gère un cycle complet pour une connexion SRT acceptée :
//  1. Crée et démarre le pipeline GStreamer (appsrc → … → appsink).
//  2. Publie le track LiveKit immédiatement (type connu depuis le streamid).
//  3. Goroutine SRT : recv → Push() dans l'appsrc.
//  4. Boucle principale : Frames() → WriteSample LiveKit.
//  5. Sur fermeture de connexion ou EOS : unpublish, Stop, Free.
func runStream(ctx context.Context, conn *SRTConn, name, source, mediaType string, room *lksdk.Room) {
	var pipelineStr string
	switch mediaType {
	case "video":
		pipelineStr = buildVideoPipeline()
	case "audio":
		pipelineStr = buildAudioPipeline()
	default:
		logger.Error("[stream:%s] Type de média inconnu : %q — connexion fermée", name, mediaType)
		conn.Close()
		return
	}

	recv, err := newGstReceiver(pipelineStr)
	if err != nil {
		logger.Error("[stream:%s] Création pipeline : %v", name, err)
		conn.Close()
		return
	}
	if err := recv.Start(); err != nil {
		logger.Error("[stream:%s] Démarrage pipeline : %v", name, err)
		recv.Free()
		conn.Close()
		return
	}

	track, pub, err := publishTrack(name, mediaType, source, room)
	if err != nil {
		logger.Error("[stream:%s] PublishTrack : %v", name, err)
		recv.Stop()
		recv.Free()
		conn.Close()
		return
	}

	// Goroutine SRT → appsrc.
	// Le goroutine possède conn et la ferme quand il se termine.
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

	// Boucle principale : appsink → LiveKit WriteSample.
loop:
	for {
		select {
		case <-ctx.Done():
			conn.Close()  // débloque le goroutine de réception SRT
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
		logger.Info("[stream:%s] Track retiré de LiveKit", name)
	}
	recv.Stop()
	recv.Free()
}

// runTelemetry lit les messages JSON d'un flux de télémétrie depuis une connexion SRT
// et met à jour les métadonnées de la room LiveKit à chaque message reçu.
// key identifie le flux ("ups", "modem", …) ; les données sont fusionnées dans meta
// avant l'envoi pour ne pas écraser les données des autres flux.
// Les métadonnées sont persistantes côté serveur : les clients qui rejoignent
// la room après un envoi voient la dernière valeur sans délai d'attente.
func runTelemetry(ctx context.Context, conn *SRTConn, key string, cfg config.Config, meta *roomMeta) {
	defer conn.Close()
	client := lksdk.NewRoomServiceClient(cfg.LiveKit.APIURL(), cfg.LiveKit.APIKey, cfg.LiveKit.APISecret)

	for {
		data, err := conn.Recv()
		if err != nil || data == nil {
			logger.Info("[telemetry:%s] Connexion fermée", key)
			return
		}

		// Valider le JSON reçu avant de l'intégrer dans les métadonnées.
		if !json.Valid(data) {
			logger.Warn("[telemetry:%s] Message non-JSON ignoré", key)
			continue
		}

		// Fusionner avec les autres flux de télémétrie déjà reçus.
		merged := meta.update(key, json.RawMessage(data))

		if _, err := client.UpdateRoomMetadata(ctx, &lkproto.UpdateRoomMetadataRequest{
			Room:     cfg.LiveKit.Room,
			Metadata: merged,
		}); err != nil && ctx.Err() == nil {
			logger.Warn("[telemetry:%s] UpdateRoom : %v", key, err)
		}
	}
}

// parseStreamID extrait le nom et la source depuis le streamid SRT.
// Format envoyé par la Jetson : "name:source" (ex : "Route:camera").
// Retourne des valeurs par défaut si le format est vide ou inattendu.
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

// mediaTypeFromSource déduit le type de média depuis la source LiveKit.
// "microphone" → "audio" (pipeline Opus), tout autre → "video" (pipeline AV1).
func mediaTypeFromSource(source string) string {
	if source == "microphone" {
		return "audio"
	}
	return "video"
}

// publishTrack crée et publie un track LiveKit selon le type de média.
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
		return nil, nil, fmt.Errorf("type de média inconnu : %q", mediaType)
	}

	track, err := lksdk.NewLocalSampleTrack(codec)
	if err != nil {
		return nil, nil, fmt.Errorf("NewLocalSampleTrack : %w", err)
	}

	pub, err := room.LocalParticipant.PublishTrack(track, &opts)
	if err != nil {
		return nil, nil, fmt.Errorf("PublishTrack : %w", err)
	}

	logger.Info("[stream:%s] Track %s publié dans LiveKit", name, mediaType)
	return track, pub, nil
}

// trackSource convertit la chaîne de source en constante proto LiveKit.
func trackSource(s string) lkproto.TrackSource {
	switch s {
	case "microphone":
		return lkproto.TrackSource_MICROPHONE
	default:
		return lkproto.TrackSource_CAMERA
	}
}
