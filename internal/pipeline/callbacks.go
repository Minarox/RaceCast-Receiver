package pipeline

// callbacks.go gère le registre des GstReceiver et les callbacks CGo exportés
// vers GStreamer pour les signaux "caller-added" / "caller-removed" du plugin SRT.
//
// Contrainte CGo : un fichier contenant //export ne peut avoir que des
// déclarations (pas de définitions) dans son preamble C. Les définitions de
// fonctions C sont dans gst.go.

// #include <stdint.h>
import "C"

import "sync"

var (
	recvMu    sync.Mutex
	recvs     = map[int64]chan callerEvent{}
	recvIDSeq int64
)

// callerEvent est envoyé depuis le callback C quand un caller SRT se connecte
// ou se déconnecte d'un port en écoute.
type callerEvent struct {
	streamID string // non vide à la connexion ; format "name:source" envoyé par la Jetson
	removed  bool   // true à la déconnexion
}

// allocReceiverID enregistre le canal d'événements du receiver et retourne un ID
// unique utilisé comme user_data dans le signal G_CALLBACK.
func allocReceiverID(ch chan callerEvent) int64 {
	recvMu.Lock()
	defer recvMu.Unlock()
	recvIDSeq++
	id := recvIDSeq
	recvs[id] = ch
	return id
}

// freeReceiverID supprime le receiver du registre (appelé par GstReceiver.Free).
func freeReceiverID(id int64) {
	recvMu.Lock()
	delete(recvs, id)
	recvMu.Unlock()
}

// GoCallerAdded est appelé depuis le callback C du signal GStreamer "caller-added".
// streamid contient la valeur SRT SRTO_STREAMID envoyée par la Jetson,
// au format "name:source" (ex : "Route:camera", "Habitacle:microphone").
// Un streamid vide signifie que la Jetson n'a pas configuré ce paramètre.
//
//export GoCallerAdded
func GoCallerAdded(id C.int64_t, streamid *C.char) {
	recvMu.Lock()
	ch, ok := recvs[int64(id)]
	recvMu.Unlock()
	if ok {
		select {
		case ch <- callerEvent{streamID: C.GoString(streamid)}:
		default:
		}
	}
}

// GoCallerRemoved est appelé depuis le callback C du signal GStreamer "caller-removed".
//
//export GoCallerRemoved
func GoCallerRemoved(id C.int64_t) {
	recvMu.Lock()
	ch, ok := recvs[int64(id)]
	recvMu.Unlock()
	if ok {
		select {
		case ch <- callerEvent{removed: true}:
		default:
		}
	}
}
