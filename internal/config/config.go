package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Config contient toute la configuration du receiver chargée depuis les variables d'environnement.
type Config struct {
	LiveKit LiveKitConfig
	SRT     SRTConfig
}

// LiveKitConfig décrit la connexion au serveur LiveKit.
type LiveKitConfig struct {
	TLS       bool   // RC_LIVEKIT_TLS (défaut true)
	Domain    string // RC_LIVEKIT_DOMAIN
	APIKey    string // RC_LIVEKIT_API_KEY
	APISecret string // RC_LIVEKIT_API_SECRET
	Room      string // RC_LIVEKIT_ROOM (défaut "racecast")
	Identity  string // RC_LIVEKIT_IDENTITY (défaut "racecast-receiver")
}

// ServerURL retourne l'URL WebSocket LiveKit.
func (c LiveKitConfig) ServerURL() string {
	scheme := "wss"
	if !c.TLS {
		scheme = "ws"
	}
	return fmt.Sprintf("%s://%s", scheme, c.Domain)
}

// APIURL retourne l'URL HTTP de l'API LiveKit.
func (c LiveKitConfig) APIURL() string {
	scheme := "https"
	if !c.TLS {
		scheme = "http"
	}
	return fmt.Sprintf("%s://%s", scheme, c.Domain)
}

// SRTConfig décrit les paramètres d'écoute SRT.
//
// RC_SRT_PORTS="9000-9010" ouvre un listener par port dans la plage.
// Chaque port accepte n'importe quel type de flux (vidéo AV1, audio Opus,
// ou autre à l'avenir). Le type est déterminé dynamiquement au moment de la
// connexion via le paramètre SRT streamid ("name:source") envoyé par la Jetson.
type SRTConfig struct {
	PortStart int // RC_SRT_PORTS : borne inférieure (incluse)
	PortEnd   int // RC_SRT_PORTS : borne supérieure (incluse)
	Latency   int // RC_SRT_LATENCY en ms (défaut 2000)
}

// Load lit la configuration depuis les variables d'environnement.
func Load() Config {
	start, end := parseSRTPorts()
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
			PortStart: start,
			PortEnd:   end,
			Latency:   getEnvInt("RC_SRT_LATENCY", 2000),
		},
	}
}

// parseSRTPorts analyse RC_SRT_PORTS="start-end" et retourne (start, end).
// Un port unique "9000" est accepté (start == end).
func parseSRTPorts() (start, end int) {
	v := os.Getenv("RC_SRT_PORTS")
	if v == "" {
		return 9000, 9007 // défaut : 8 ports
	}
	if i := strings.Index(v, "-"); i >= 0 {
		s, err1 := strconv.Atoi(strings.TrimSpace(v[:i]))
		e, err2 := strconv.Atoi(strings.TrimSpace(v[i+1:]))
		if err1 == nil && err2 == nil && s > 0 && e >= s {
			return s, e
		}
	}
	// Port unique
	if n, err := strconv.Atoi(strings.TrimSpace(v)); err == nil && n > 0 {
		return n, n
	}
	return 9000, 9007
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
