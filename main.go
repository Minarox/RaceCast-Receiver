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

	// Silence internal pion/WebRTC logs (very verbose).
	// log.SetOutput(io.Discard) suppresses the standard logger for the whole process.
	log.SetOutput(io.Discard)
	lkprotoLogger.SetLogger(nullLogger{}, "racecast-receiver")

	if err := pipeline.CheckGStreamer(); err != nil {
		logger.Fatal("GStreamer : %v", err)
	}

	if cfg.LiveKit.Domain == "" {
		logger.Fatal("RC_LIVEKIT_DOMAIN not set")
	}

	ensureRoom(cfg)

	room, err := connectLiveKit(cfg)
	if err != nil {
		logger.Fatal("LiveKit connect : %v", err)
	}
	defer room.Disconnect()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	var wg sync.WaitGroup
	pipeline.RunStreams(ctx, cfg, room, &wg)

	logger.Info("Receiver started — SRT port %d. Ctrl+C to stop.", cfg.SRT.Port)
	<-ctx.Done()
	logger.Info("Signal received — shutting down...")

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		logger.Warn("Timeout — forcing exit")
	}
	logger.Info("Shutdown complete.")
}

// ensureRoom creates the LiveKit room if it does not already exist.
func ensureRoom(cfg config.Config) {
	client := lksdk.NewRoomServiceClient(cfg.LiveKit.APIURL(), cfg.LiveKit.APIKey, cfg.LiveKit.APISecret)
	_, err := client.CreateRoom(context.Background(), &livekit.CreateRoomRequest{
		Name:             cfg.LiveKit.Room,
		DepartureTimeout: uint32((24 * time.Hour).Seconds()),
	})
	if err != nil {
		logger.Info("[livekit] Room %q already exists or created", cfg.LiveKit.Room)
	} else {
		logger.Info("[livekit] Room %q created (DepartureTimeout=24h)", cfg.LiveKit.Room)
	}
}

// connectLiveKit connects to the LiveKit room and returns the Room client.
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

	logger.Info("[livekit] Connected to room %q as %q", cfg.LiveKit.Room, cfg.LiveKit.Identity)
	return room, nil
}

// nullLogger implements protoLogger.Logger by discarding all pion/WebRTC messages.
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
