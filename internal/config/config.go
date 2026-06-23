package config

import (
	"fmt"
	"os"
	"strconv"
)

// Config holds the full receiver configuration loaded from environment variables.
type Config struct {
	LiveKit LiveKitConfig
	SRT     SRTConfig
}

// LiveKitConfig describes the LiveKit server connection.
type LiveKitConfig struct {
	TLS       bool   // RC_LIVEKIT_TLS (default true)
	Domain    string // RC_LIVEKIT_DOMAIN
	APIKey    string // RC_LIVEKIT_API_KEY
	APISecret string // RC_LIVEKIT_API_SECRET
	Room      string // RC_LIVEKIT_ROOM (default "racecast")
	Identity  string // RC_LIVEKIT_IDENTITY (default "racecast-receiver")
}

// ServerURL returns the LiveKit WebSocket URL.
func (c LiveKitConfig) ServerURL() string {
	scheme := "wss"
	if !c.TLS {
		scheme = "ws"
	}
	return fmt.Sprintf("%s://%s", scheme, c.Domain)
}

// APIURL returns the LiveKit HTTP API URL.
func (c LiveKitConfig) APIURL() string {
	scheme := "https"
	if !c.TLS {
		scheme = "http"
	}
	return fmt.Sprintf("%s://%s", scheme, c.Domain)
}

// SRTConfig describes the SRT listener. One port for all connections;
// stream type is determined from the SRT streamid ("name:source").
type SRTConfig struct {
	Port    int // RC_SRT_PORT: listen port
	Latency int // RC_SRT_LATENCY in ms (default 800)
}

// Load reads configuration from environment variables.
func Load() Config {
	return Config{
		LiveKit: LiveKitConfig{
			TLS:       getEnv("RC_LIVEKIT_TLS", "true") != "false",
			Domain:    getEnv("RC_LIVEKIT_DOMAIN", ""),
			APIKey:    getEnv("RC_LIVEKIT_API_KEY", ""),
			APISecret: getEnv("RC_LIVEKIT_API_SECRET", ""),
			Room:      getEnv("RC_LIVEKIT_ROOM", "racecast"),
			Identity:  getEnv("RC_LIVEKIT_IDENTITY", "racecast-receiver"),
		},
		SRT: SRTConfig{
			Port:    parseSRTPort(),
			Latency: getEnvInt("RC_SRT_LATENCY", 800),
		},
	}
}

// parseSRTPort reads RC_SRT_PORT and returns the configured port (default 9000).
func parseSRTPort() int {
	if n, err := strconv.Atoi(os.Getenv("RC_SRT_PORT")); err == nil && n > 0 {
		return n
	}
	return 9000
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getEnvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
