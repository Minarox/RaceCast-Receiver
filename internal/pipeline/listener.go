package pipeline

// listener.go implements a multi-connection SRT listener via libsrt.
// Unlike srtsrc (one element per connection), this listener accepts N connections
// on a single port, routing by SRT streamid read at handshake.

// #cgo pkg-config: srt
// #include <srt/srt.h>
// #include <netinet/in.h>
// #include <string.h>
// #include <stdlib.h>
//
// // srt_new_listener creates a dual-stack (IPv4 + IPv6) SRT listener socket.
// // Returns SRT_INVALID_SOCK on error.
// static SRTSOCKET srt_new_listener(int port, int latency, const char *passphrase) {
//     srt_startup(); // idempotent, safe to call multiple times
//     SRTSOCKET s = srt_create_socket();
//     if (s == SRT_INVALID_SOCK) return SRT_INVALID_SOCK;
//
//     // Dual-stack: accept IPv4 and IPv6 on the same socket.
//     int no = 0;
//     srt_setsockflag(s, SRTO_IPV6ONLY, &no, sizeof(no));
//
//     // Receive latency (inherited by accepted connections).
//     int lat = latency;
//     srt_setsockflag(s, SRTO_RCVLATENCY, &lat, sizeof(lat));
//
//     // Passphrase authentication: reject connections with wrong key (AES-256).
//     if (passphrase && strlen(passphrase) >= 10) {
//         srt_setsockflag(s, SRTO_PASSPHRASE, passphrase, (int)strlen(passphrase));
//         int pbkeylen = 32;
//         srt_setsockflag(s, SRTO_PBKEYLEN, &pbkeylen, sizeof(pbkeylen));
//     }
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
// // srt_do_accept waits for an incoming connection and reads its streamid.
// // Returns SRT_INVALID_SOCK if the listener is closed or on error.
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
// // srt_do_recv reads the next SRT message.
// // Returns >0=bytes received, 0=connection closed, <0=error.
// static int srt_do_recv(SRTSOCKET sock, char *buf, int maxlen) {
//     return srt_recvmsg(sock, buf, maxlen);
// }
//
// // srt_do_close closes an SRT socket.
// static void srt_do_close(SRTSOCKET sock) {
//     srt_close(sock);
// }
//
// // srt_is_invalid returns 1 if the socket is invalid.
// static int srt_is_invalid(SRTSOCKET sock) {
//     return sock == SRT_INVALID_SOCK ? 1 : 0;
// }
//
// // srt_do_send sends a message to the connected SRT peer (bidirectional SRT).
// // Returns >0=bytes sent, <0=error.
// static int srt_do_send(SRTSOCKET sock, const char *buf, int len) {
//     return srt_sendmsg(sock, buf, len, -1, 0);
// }
//
// // srt_get_stats fills packet-loss %, RTT (ms) and bandwidth (Mbps) from
// // interval SRT receive statistics (cleared after each call).
// static void srt_get_stats(SRTSOCKET sock,
//                           double *loss_pct, double *rtt_ms, double *bandwidth_mbps) {
//     SRT_TRACEBSTATS st;
//     *loss_pct = 0; *rtt_ms = 0; *bandwidth_mbps = 0;
//     if (srt_bistats(sock, &st, 1, 0) == SRT_ERROR) return;
//     *rtt_ms         = st.msRTT;
//     *bandwidth_mbps = st.mbpsBandwidth;
//     int64_t total = st.pktRecv + (int64_t)st.pktRcvLoss;
//     if (total > 0)
//         *loss_pct = 100.0 * (double)st.pktRcvLoss / (double)total;
// }
import "C"

import (
	"fmt"
	"os"
	"strings"
	"unsafe"
)

// recvBufSize is the max SRT message size. 32 KB covers AV1 IDR frames at ≤ 1 Mbps
// (IDR frames are typically 5–15× the average P-frame; without intra-refresh they
// can reach ~40 KB). SRT truncates silently when the buffer is too small, which
// would corrupt the bitstream. Opus frames are always < 4 KB.
const recvBufSize = 1 << 15 // 32 KB

// SRTListener wraps an SRT socket in listener mode (single port, N connections).
type SRTListener struct {
	sock C.SRTSOCKET
}

// newSRTListener creates a dual-stack SRT listener on the given port.
// Passphrase from RC_SRT_PASSPHRASE; if set, AES-256 is required on all connections.
func newSRTListener(port, latency int) (*SRTListener, error) {
	passphrase := strings.TrimSpace(os.Getenv("RC_SRT_PASSPHRASE"))
	cPass := C.CString(passphrase)
	defer C.free(unsafe.Pointer(cPass))
	sock := C.srt_new_listener(C.int(port), C.int(latency), cPass)
	if C.srt_is_invalid(sock) != 0 {
		return nil, fmt.Errorf("srt_new_listener on port %d: %s", port, C.GoString(C.srt_getlasterror_str()))
	}
	return &SRTListener{sock: sock}, nil
}

// Accept waits for and accepts the next incoming connection.
// Blocks until a connection arrives or the listener is closed.
// Returns the SRT streamid sent by the caller during the handshake.
func (l *SRTListener) Accept() (*SRTConn, string, error) {
	var buf [512]C.char
	sock := C.srt_do_accept(l.sock, &buf[0], 512)
	if C.srt_is_invalid(sock) != 0 {
		return nil, "", fmt.Errorf("srt_accept: %s", C.GoString(C.srt_getlasterror_str()))
	}
	return &SRTConn{sock: sock}, C.GoString(&buf[0]), nil
}

// Close closes the listener and unblocks any pending Accept().
func (l *SRTListener) Close() {
	C.srt_do_close(l.sock)
}

// SRTConn wraps an SRT connection accepted by SRTListener.
type SRTConn struct {
	sock C.SRTSOCKET
}

// Recv reads the next SRT message from the connection.
// Returns (nil, nil) when the connection is cleanly closed.
func (c *SRTConn) Recv() ([]byte, error) {
	buf := make([]byte, recvBufSize)
	n := C.srt_do_recv(c.sock, (*C.char)(unsafe.Pointer(&buf[0])), C.int(recvBufSize))
	if n == 0 {
		return nil, nil // connection cleanly closed
	}
	if n < 0 {
		return nil, fmt.Errorf("srt_recvmsg: %s", C.GoString(C.srt_getlasterror_str()))
	}
	return buf[:int(n)], nil
}

// Close closes the SRT connection.
func (c *SRTConn) Close() {
	C.srt_do_close(c.sock)
}

// Send sends data to the emitter (SRT connections are bidirectional).
func (c *SRTConn) Send(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	n := C.srt_do_send(c.sock, (*C.char)(unsafe.Pointer(&data[0])), C.int(len(data)))
	if n < 0 {
		return fmt.Errorf("srt_sendmsg: %s", C.GoString(C.srt_getlasterror_str()))
	}
	return nil
}

// Stats returns interval packet-loss (%), RTT (ms) and bandwidth (Mbps); auto-cleared.
func (c *SRTConn) Stats() (lossPct, rttMS, bandwidthMbps float64) {
	var lp, rtt, bw C.double
	C.srt_get_stats(c.sock, &lp, &rtt, &bw)
	return float64(lp), float64(rtt), float64(bw)
}
