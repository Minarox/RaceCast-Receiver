package pipeline

// receiver.go orchestre les listeners SRT et la publication dans LiveKit.
//
// Architecture en deux phases par port :
//  1. Probe  : pipeline léger "srtsrc ! fakesink" — lie le port SRT et attend
//              la connexion de la Jetson. Le signal caller-added fournit le
//              streamid au format "name:source" (ex : "Route:camera").
//              Le probe s'arrête dès le premier caller-added.
//  2. Stream : pipeline typé (av1parse ou queue) créé selon la source détectée.
//              La Jetson se reconnecte automatiquement (mode caller SRT).
//              Le track LiveKit est publié à la deuxième connexion, puis retiré
//              à la déconnexion. Le cycle recommence indéfiniment.
//
// Tous les ports de la plage RC_SRT_PORTS sont homogènes : le type (vidéo, audio,
// ou tout futur format) est découvert dynamiquement depuis le streamid SRT.
// Aucune configuration par flux n'est nécessaire côté receiver.

import (
	"context"
	"fmt"
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

// srtListenerURI construit l'URI SRT pour un listener dual-stack (IPv4 + IPv6) sur le port donné.
// Sur Linux, lier à [::] avec IPV6_V6ONLY=0 (valeur système par défaut) crée un socket
// dual-stack qui accepte à la fois les connexions IPv4 et IPv6.
func srtListenerURI(port, latency int) string {
	return fmt.Sprintf("srt://[::]:%d?mode=listener&latency=%d", port, latency)
}

// buildProbePipeline construit le pipeline GStreamer minimal pour détecter le
// streamid SRT d'une connexion entrante sans consommer les données.
// L'élément srtsrc est nommé "srcsrc" pour que connect_caller_signals le trouve.
func buildProbePipeline(port, latency int) string {
	return fmt.Sprintf(`srtsrc name=srcsrc uri="%s" ! fakesink sync=false`, srtListenerURI(port, latency))
}

// buildVideoRecvPipeline construit le pipeline GStreamer pour recevoir de l'AV1 via SRT.
//
// La Jetson envoie de l'AV1 OBU stream brut (nvv4l2av1enc → av1parse → srtsink).
// av1parse côté receiver resynchronise sur les OBU headers et aligne sur les
// temporal units (alignment=tu) : chaque buffer appsink = une trame AV1 complète
// prête pour WriteSample LiveKit.
func buildVideoRecvPipeline(port, latency int) string {
	return fmt.Sprintf(
		`srtsrc name=srcsrc uri="%s" ! `+
			`av1parse ! video/x-av1,stream-format=obu-stream,alignment=tu ! `+
			`appsink name=sink max-buffers=4 drop=true sync=false`,
		srtListenerURI(port, latency),
	)
}

// buildAudioRecvPipeline construit le pipeline GStreamer pour recevoir de l'Opus via SRT.
//
// La Jetson envoie des trames Opus brutes (opusenc → srtsink).
// SRT étant orienté message, chaque message SRT = un buffer GStreamer = une trame Opus.
func buildAudioRecvPipeline(port, latency int) string {
	return fmt.Sprintf(
		`srtsrc name=srcsrc uri="%s" ! `+
			`queue max-buffers=16 leaky=downstream ! `+
			`appsink name=sink max-buffers=16 drop=true sync=false`,
		srtListenerURI(port, latency),
	)
}

// RunStreams démarre un listener SRT par port de la plage cfg.SRT et les publie
// dans room. Chaque port est totalement autonome et se reconnecte automatiquement.
// Bloque jusqu'à l'annulation de ctx.
func RunStreams(ctx context.Context, cfg config.Config, room *lksdk.Room, wg *sync.WaitGroup) {
	for port := cfg.SRT.PortStart; port <= cfg.SRT.PortEnd; port++ {
		p := port
		wg.Add(1)
		go func() {
			defer wg.Done()
			runStreamLoop(ctx, p, cfg.SRT.Latency, room)
		}()
	}
}

// runStreamLoop boucle indéfiniment pour un port donné :
//  1. Lance un probe pour détecter le type de flux (phase 1).
//  2. Lance le vrai pipeline typé et publie dans LiveKit (phase 2).
//  3. Recommence dès la déconnexion de la Jetson.
func runStreamLoop(ctx context.Context, port, latency int, room *lksdk.Room) {
	for {
		if ctx.Err() != nil {
			return
		}

		// Phase 1 : probe — lire le streamid pour connaître le type de flux.
		logger.Info("[stream:port=%d] En attente de connexion SRT (probe)...", port)
		mediaType, err := probeStreamID(ctx, port, latency)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			logger.Error("[stream:port=%d] Probe : %v — nouvelle tentative dans 2s", port, err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
			}
			continue
		}

		// Phase 2 : pipeline typé + publication LiveKit.
		// La Jetson se reconnecte automatiquement après la fin du probe.
		logger.Info("[stream:port=%d] Type détecté : %s — démarrage du pipeline", port, mediaType)
		if err := runOnce(ctx, port, mediaType, latency, room); err != nil {
			if ctx.Err() != nil {
				return
			}
			logger.Error("[stream:port=%d] %v — nouvelle tentative dans 2s", port, err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
			}
		}
	}
}

// probeStreamID démarre un pipeline probe sur le port, attend le premier
// caller-added SRT, lit le streamid et retourne le type de média correspondant.
// Le probe s'arrête immédiatement après la détection, libérant le port pour
// le vrai pipeline. La Jetson (en mode caller) se reconnecte automatiquement.
func probeStreamID(ctx context.Context, port, latency int) (mediaType string, err error) {
	probe, err := newGstProbe(buildProbePipeline(port, latency))
	if err != nil {
		return "", fmt.Errorf("création probe : %w", err)
	}
	defer probe.Free()

	if err := probe.Start(); err != nil {
		return "", fmt.Errorf("démarrage probe : %w", err)
	}
	defer probe.Stop()

	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case err := <-probe.ErrCh():
		return "", err
	case ev := <-probe.CallerEvents():
		_, source := parseStreamID(ev.streamID)
		return mediaTypeFromSource(source), nil
	}
}

// runOnce gère un cycle complet sur le pipeline typé :
//  1. Crée et démarre le pipeline (lie le port SRT).
//  2. Attend caller-added → lit le streamid → publie le track LiveKit.
//  3. Formate les frames vers WriteSample LiveKit.
//  4. Sur caller-removed, EOS ou erreur : retire le track et retourne.
func runOnce(ctx context.Context, port int, mediaType string, latency int, room *lksdk.Room) error {
	var pipelineStr string
	switch mediaType {
	case "video":
		pipelineStr = buildVideoRecvPipeline(port, latency)
	case "audio":
		pipelineStr = buildAudioRecvPipeline(port, latency)
	default:
		return fmt.Errorf("type de flux inconnu : %q", mediaType)
	}

	recv, err := newGstReceiver(pipelineStr)
	if err != nil {
		return fmt.Errorf("création pipeline : %w", err)
	}
	defer recv.Free()

	disconnectCh := make(chan struct{}, 1)
	recv.SetOnDisconnect(func() {
		select {
		case disconnectCh <- struct{}{}:
		default:
		}
	})

	if err := recv.Start(); err != nil {
		return fmt.Errorf("démarrage pipeline : %w", err)
	}
	defer recv.Stop()

	var (
		track *lksdk.LocalSampleTrack
		pub   *lksdk.LocalTrackPublication
		name  string
	)

	unpublish := func() {
		if pub != nil {
			_ = room.LocalParticipant.UnpublishTrack(pub.SID())
			logger.Info("[stream:%s] Track retiré de LiveKit", name)
			pub = nil
			track = nil
		}
	}

	for {
		select {
		case <-ctx.Done():
			unpublish()
			return nil

		case <-disconnectCh:
			unpublish()
			return nil

		case ev, ok := <-recv.CallerEvents():
			if !ok {
				unpublish()
				return nil
			}
			if ev.removed {
				unpublish()
				continue
			}
			// caller-added : publier le track LiveKit.
			parsedName, source := parseStreamID(ev.streamID)
			name = parsedName
			logger.Info("[stream:%s] Jetson connecté (port %d) — publication dans LiveKit", name, port)
			var pubErr error
			track, pub, pubErr = publishTrack(name, mediaType, source, room)
			if pubErr != nil {
				logger.Error("[stream:port=%d] PublishTrack : %v", port, pubErr)
			}

		case f, ok := <-recv.Frames():
			if !ok {
				unpublish()
				return nil
			}
			if track == nil {
				continue
			}
			if err := track.WriteSample(media.Sample{
				Data:     f.Data,
				Duration: f.Duration,
			}, nil); err != nil {
				logger.Warn("[stream:%s] WriteSample : %v", name, err)
			}
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

// mediaTypeFromSource déduit le type de média GStreamer depuis la source LiveKit.
// "microphone" → "audio" (pipeline Opus), tout autre → "video" (pipeline AV1).
// Extensible : ajouter une nouvelle source ici pour supporter un nouveau type de flux.
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
