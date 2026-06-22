package logger

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

const logsDir = "logs"

var (
	mu          sync.Mutex
	logFile     *os.File
	currentDate string
	fileEnabled bool

	// consoleOut est un *os.File ouvert sur un duplicata de fd 1 créé au démarrage
	// du programme, avant tout appel GStreamer. Quand le package pipeline redirige
	// temporairement fd 1 vers /dev/null (pour masquer les messages des drivers
	// Nvidia), consoleOut continue d'écrire sur le terminal d'origine.
	consoleOut *os.File

	infoFn  func(string, ...any)
	warnFn  func(string, ...any)
	errorFn func(string, ...any)
	fatalFn func(string, ...any)
)

func init() {
	// dup(1) crée un nouveau descripteur pointant vers le même terminal que fd 1.
	// Ce descripteur n'est pas affecté par dup2(/dev/null, 1) appelé plus tard.
	if fd, err := syscall.Dup(int(os.Stdout.Fd())); err == nil {
		syscall.CloseOnExec(fd) // ne pas hériter dans les processus fils
		consoleOut = os.NewFile(uintptr(fd), "stdout")
	} else {
		consoleOut = os.Stdout
	}
}

// InitConsole initialise le logger en mode console uniquement (pas de fichier).
// À utiliser pour les commandes interactives comme --ups.
func InitConsole() {
	setup(false)
}

// Init initialise le logger avec rotation automatique par jour.
// Retourne une fonction de fermeture à appeler en fin de programme.
func Init() func() {
	if err := os.MkdirAll(logsDir, 0o755); err != nil {
		log.Fatalf("Impossible de créer le dossier de logs : %v", err)
	}

	setup(true)

	// Ouvrir le fichier du jour immédiatement.
	mu.Lock()
	now := time.Now()
	rotateIfNeeded(now)
	logPath := filepath.Join(logsDir, now.Format("2006-01-02"), "receiver.log")
	mu.Unlock()

	Info("Logs enregistrés dans %s", logPath)
	return func() {
		mu.Lock()
		defer mu.Unlock()
		if logFile != nil {
			logFile.Close()
			logFile = nil
		}
	}
}

// rotateIfNeeded ouvre un nouveau fichier de log si la date a changé.
// Doit être appelé avec mu verrouillé.
func rotateIfNeeded(now time.Time) {
	today := now.Format("2006-01-02")
	if today == currentDate {
		return
	}

	if logFile != nil {
		logFile.Close()
		logFile = nil
	}

	dayDir := filepath.Join(logsDir, today)
	if err := os.MkdirAll(dayDir, 0o755); err != nil {
		fmt.Fprintf(consoleOut, "%s | \033[33mWARN \033[0m Impossible de créer %s : %v\n",
			now.Format("2006-01-02 15:04:05"), dayDir, err)
		return
	}

	logPath := filepath.Join(dayDir, "receiver.log")
	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		fmt.Fprintf(consoleOut, "%s | \033[33mWARN \033[0m Impossible d'ouvrir %s : %v\n",
			now.Format("2006-01-02 15:04:05"), logPath, err)
		return
	}

	wasRunning := currentDate != ""
	logFile = f
	currentDate = today

	// Notifier la rotation en cours d'exécution (pas au premier démarrage,
	// car Init() envoie lui-même le message via Info()).
	if wasRunning {
		fmt.Fprintf(consoleOut, "%s | \033[36mINFO \033[0m Rotation des logs → %s\n",
			now.Format("2006-01-02 15:04:05"), logPath)
		fmt.Fprintf(logFile, "%s | INFO  Rotation des logs → %s\n",
			now.Format("2006/01/02 15:04:05"), logPath)
	}
}

func setup(withFile bool) {
	fileEnabled = withFile

	write := func(out *os.File, level, ansiLevel, msg string) {
		now := time.Now()
		mu.Lock()
		defer mu.Unlock()
		fmt.Fprintf(out, "%s | %s%s\033[0m %s\n", now.Format("2006-01-02 15:04:05"), ansiLevel, level, msg)
		if fileEnabled {
			rotateIfNeeded(now)
			if logFile != nil {
				fmt.Fprintf(logFile, "%s | %s %s\n", now.Format("2006/01/02 15:04:05"), level, msg)
			}
		}
	}

	infoFn = func(format string, v ...any) {
		write(consoleOut, "INFO ", "\033[36m", fmt.Sprintf(format, v...))
	}
	warnFn = func(format string, v ...any) {
		write(consoleOut, "WARN ", "\033[33m", fmt.Sprintf(format, v...))
	}
	errorFn = func(format string, v ...any) {
		write(consoleOut, "ERROR", "\033[31m", fmt.Sprintf(format, v...))
	}
	fatalFn = func(format string, v ...any) {
		write(consoleOut, "FATAL", "\033[1;31m", fmt.Sprintf(format, v...))
		os.Exit(1)
	}
}

func Info(format string, v ...any)  { infoFn(format, v...) }
func Warn(format string, v ...any)  { warnFn(format, v...) }
func Error(format string, v ...any) { errorFn(format, v...) }
func Fatal(format string, v ...any) { fatalFn(format, v...) }

