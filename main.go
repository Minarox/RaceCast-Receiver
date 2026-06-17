package main

import (
	"context"
	"io"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	lkauth "github.com/livekit/protocol/auth"
	"github.com/livekit/protocol/livekit"
	lkprotoLogger "github.com/livekit/protocol/logger"
	lksdk "github.com/livekit/server-sdk-go/v2"

	"racecast-receiver/internal/config"
	"racecast-receiver/internal/env"
	"racecast-receiver/internal/logger"
	"racecast-receiver/internal/pipeline"
)

func main() {
	env.Load(".env")

	cfg := config.Load()

	closeLog := logger.Init()
	defer closeLog()

	// Supprimer les logs internes de pion/WebRTC (très verbeux).
	// log.SetOutput(io.Discard) coupe le logger standard pour toute la durée du processus.
	// NE PAS restaurer avec log.SetOutput(os.Stdout) : pion/ICE utilise log.Printf
	// pour ses messages "Failed to send packet" et "ICE connection state changed".
	log.SetOutput(io.Discard)
	lkprotoLogger.SetLogger(nullLogger{}, "racecast-receiver")

	if cfg.LiveKit.Domain == "" {
		logger.Fatal("RC_LIVEKIT_DOMAIN non défini")
	}

	if err := ensureRoom(cfg); err != nil {
		logger.Fatal("LiveKit ensureRoom : %v", err)
	}

	room, err := connectLiveKit(cfg)
	if err != nil {
		logger.Fatal("LiveKit connect : %v", err)
	}
	defer room.Disconnect()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	var wg sync.WaitGroup
	pipeline.RunStreams(ctx, cfg, room, &wg)

	total := cfg.SRT.PortEnd - cfg.SRT.PortStart + 1
	logger.Info("Receiver démarré — plage SRT %d-%d (%d port(s)). Ctrl+C pour arrêter.", cfg.SRT.PortStart, cfg.SRT.PortEnd, total)
	<-ctx.Done()
	logger.Info("Signal reçu — arrêt en cours...")

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		logger.Warn("Timeout — arrêt forcé")
	}
	logger.Info("Arrêt complet.")
}

// ensureRoom crée la room LiveKit si elle n'existe pas encore.
func ensureRoom(cfg config.Config) error {
	client := lksdk.NewRoomServiceClient(cfg.LiveKit.APIURL(), cfg.LiveKit.APIKey, cfg.LiveKit.APISecret)
	_, err := client.CreateRoom(context.Background(), &livekit.CreateRoomRequest{
		Name:             cfg.LiveKit.Room,
		DepartureTimeout: uint32((24 * time.Hour).Seconds()),
	})
	if err != nil {
		logger.Info("[livekit] Room %q déjà existante ou créée", cfg.LiveKit.Room)
	} else {
		logger.Info("[livekit] Room %q créée (DepartureTimeout=24h)", cfg.LiveKit.Room)
	}
	return nil
}

// connectLiveKit se connecte à la room LiveKit et retourne le client Room.
func connectLiveKit(cfg config.Config) (*lksdk.Room, error) {
	at := lkauth.NewAccessToken(cfg.LiveKit.APIKey, cfg.LiveKit.APISecret)
	grant := &lkauth.VideoGrant{
		RoomJoin: true,
		Room:     cfg.LiveKit.Room,
	}
	at.SetVideoGrant(grant).
		SetIdentity(cfg.LiveKit.Identity).
		SetValidFor(24 * time.Hour)

	token, err := at.ToJWT()
	if err != nil {
		return nil, err
	}

	room, err := lksdk.ConnectToRoomWithToken(
		cfg.LiveKit.ServerURL(),
		token,
		&lksdk.RoomCallback{},
		lksdk.WithLogger(nullLogger{}),
	)
	if err != nil {
		return nil, err
	}

	logger.Info("[livekit] Connecté à la room %q en tant que %q", cfg.LiveKit.Room, cfg.LiveKit.Identity)
	return room, nil
}

// nullLogger implémente protoLogger.Logger en ignorant tous les messages de pion/WebRTC.
type nullLogger struct{}

func (nullLogger) Debugw(_ string, _ ...any)         {}
func (nullLogger) Infow(_ string, _ ...any)          {}
func (nullLogger) Warnw(_ string, _ error, _ ...any) {}
func (nullLogger) Errorw(_ string, _ error, _ ...any) {}
func (n nullLogger) WithValues(_ ...any) lkprotoLogger.Logger    { return n }
func (n nullLogger) WithUnlikelyValues(_ ...any) lkprotoLogger.UnlikelyLogger {
	return lkprotoLogger.NewUnlikelyLogger(n)
}
func (n nullLogger) WithName(_ string) lkprotoLogger.Logger       { return n }
func (n nullLogger) WithComponent(_ string) lkprotoLogger.Logger  { return n }
func (n nullLogger) WithCallDepth(_ int) lkprotoLogger.Logger     { return n }
func (n nullLogger) WithItemSampler() lkprotoLogger.Logger        { return n }
func (n nullLogger) WithoutSampler() lkprotoLogger.Logger         { return n }
func (n nullLogger) WithDeferredValues() (lkprotoLogger.Logger, lkprotoLogger.DeferredFieldResolver) {
	return n, nullResolver{}
}

type nullResolver struct{}

func (nullResolver) Resolve(_ ...any) {}
func (nullResolver) Reset()           {}
