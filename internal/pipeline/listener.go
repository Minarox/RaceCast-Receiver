package pipeline

// listener.go implémente le listener SRT multi-connexions via libsrt directement.
//
// Contrairement à srtsrc GStreamer (une connexion à la fois par élément),
// ce listener accepte N connexions simultanées sur un seul port et les route
// par streamid SRT. Chaque connexion acceptée retourne son streamid immédiatement.

// #cgo pkg-config: srt
// #include <srt/srt.h>
// #include <netinet/in.h>
// #include <string.h>
// #include <stdlib.h>
//
// // srt_new_listener crée un socket SRT listener dual-stack (IPv4 + IPv6).
// // Retourne SRT_INVALID_SOCK en cas d'erreur.
// static SRTSOCKET srt_new_listener(int port, int latency) {
//     srt_startup(); // idempotent, safe à appeler plusieurs fois
//     SRTSOCKET s = srt_create_socket();
//     if (s == SRT_INVALID_SOCK) return SRT_INVALID_SOCK;
//
//     // Dual-stack : accepter IPv4 et IPv6 sur le même socket
//     int no = 0;
//     srt_setsockflag(s, SRTO_IPV6ONLY, &no, sizeof(no));
//
//     // Latence de réception (héritée par les connexions acceptées)
//     int lat = latency;
//     srt_setsockflag(s, SRTO_RCVLATENCY, &lat, sizeof(lat));
//
//     struct sockaddr_in6 addr;
//     memset(&addr, 0, sizeof(addr));
//     addr.sin6_family = AF_INET6;
//     addr.sin6_port   = htons((uint16_t)port);
//     addr.sin6_addr   = in6addr_any;
//
//     if (srt_bind(s, (struct sockaddr*)&addr, sizeof(addr)) == SRT_ERROR ||
//         srt_listen(s, 64) == SRT_ERROR) {
//         srt_close(s);
//         return SRT_INVALID_SOCK;
//     }
//     return s;
// }
//
// // srt_do_accept attend une connexion entrante et lit son streamid.
// // Retourne SRT_INVALID_SOCK si le listener est fermé ou en erreur.
// static SRTSOCKET srt_do_accept(SRTSOCKET listener, char *out, int maxlen) {
//     struct sockaddr_storage addr;
//     int addrlen = sizeof(addr);
//     SRTSOCKET conn = srt_accept(listener, (struct sockaddr*)&addr, &addrlen);
//     if (conn == SRT_INVALID_SOCK) return SRT_INVALID_SOCK;
//     memset(out, 0, maxlen);
//     int buflen = maxlen - 1;
//     srt_getsockflag(conn, SRTO_STREAMID, out, &buflen);
//     return conn;
// }
//
// // srt_do_recv lit le prochain message SRT.
// // Retourne >0=octets reçus, 0=connexion fermée, <0=erreur.
// static int srt_do_recv(SRTSOCKET sock, char *buf, int maxlen) {
//     return srt_recvmsg(sock, buf, maxlen);
// }
//
// // srt_do_close ferme un socket SRT.
// static void srt_do_close(SRTSOCKET sock) {
//     srt_close(sock);
// }
//
// // srt_is_invalid retourne 1 si le socket est invalide (SRT_INVALID_SOCK est une macro).
// static int srt_is_invalid(SRTSOCKET sock) {
//     return sock == SRT_INVALID_SOCK ? 1 : 0;
// }
import "C"

import (
	"fmt"
	"unsafe"
)

// recvBufSize est la taille maximale d'un message SRT.
// SRT garantit que chaque srt_recvmsg retourne un message complet.
// La MTU SRT est ~1316 octets, mais les messages peuvent être fragmentés
// et réassemblés jusqu'à cette limite.
const recvBufSize = 1500 * 7 // ~10 Ko, largement suffisant pour une trame AV1/Opus

// SRTListener encapsule un socket SRT en mode listener (port unique, N connexions).
type SRTListener struct {
	sock C.SRTSOCKET
}

// newSRTListener crée un listener SRT dual-stack sur le port donné.
// latency est la latence SRT en millisecondes (héritée par les connexions entrantes).
func newSRTListener(port, latency int) (*SRTListener, error) {
	sock := C.srt_new_listener(C.int(port), C.int(latency))
	if C.srt_is_invalid(sock) != 0 {
		return nil, fmt.Errorf("srt_new_listener sur le port %d : %s", port, C.GoString(C.srt_getlasterror_str()))
	}
	return &SRTListener{sock: sock}, nil
}

// Accept attend et accepte la prochaine connexion entrante.
// Bloque jusqu'à une connexion ou jusqu'à la fermeture du listener (Close).
// Retourne le streamid SRT envoyé par le caller lors du handshake.
func (l *SRTListener) Accept() (*SRTConn, string, error) {
	var buf [512]C.char
	sock := C.srt_do_accept(l.sock, &buf[0], 512)
	if C.srt_is_invalid(sock) != 0 {
		return nil, "", fmt.Errorf("srt_accept : %s", C.GoString(C.srt_getlasterror_str()))
	}
	return &SRTConn{sock: sock}, C.GoString(&buf[0]), nil
}

// Close ferme le listener et débloque tout Accept() en attente.
func (l *SRTListener) Close() {
	C.srt_do_close(l.sock)
}

// SRTConn encapsule une connexion SRT acceptée par SRTListener.
type SRTConn struct {
	sock C.SRTSOCKET
}

// Recv lit le prochain message SRT de la connexion.
// Retourne (nil, nil) quand la connexion est fermée proprement.
// Retourne (nil, err) en cas d'erreur.
func (c *SRTConn) Recv() ([]byte, error) {
	buf := make([]byte, recvBufSize)
	n := C.srt_do_recv(c.sock, (*C.char)(unsafe.Pointer(&buf[0])), C.int(recvBufSize))
	if n == 0 {
		return nil, nil // connexion fermée proprement
	}
	if n < 0 {
		return nil, fmt.Errorf("srt_recvmsg : %s", C.GoString(C.srt_getlasterror_str()))
	}
	return buf[:int(n)], nil
}

// Close ferme la connexion SRT.
func (c *SRTConn) Close() {
	C.srt_do_close(c.sock)
}
