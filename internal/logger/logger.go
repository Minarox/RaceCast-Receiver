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

	// consoleOut is a dup of fd 1 created before any GStreamer call.
	// It keeps writing to the original terminal even when the pipeline package
	// temporarily redirects fd 1 to /dev/null to silence Nvidia driver noise.
	consoleOut *os.File

	infoFn  func(string, ...any)
	warnFn  func(string, ...any)
	errorFn func(string, ...any)
	fatalFn func(string, ...any)
)

func init() {
	// dup(1) creates a new fd pointing to the same terminal as fd 1,
	// unaffected by later dup2(/dev/null, 1) calls.
	if fd, err := syscall.Dup(int(os.Stdout.Fd())); err == nil {
		syscall.CloseOnExec(fd) // do not inherit in child processes
		consoleOut = os.NewFile(uintptr(fd), "stdout")
	} else {
		consoleOut = os.Stdout
	}
}

// InitConsole initializes the logger in console-only mode (no log file).
func InitConsole() {
	setup(false)
}

// Init initializes the logger with daily log rotation.
// Returns a close function to call at program exit.
func Init() func() {
	if err := os.MkdirAll(logsDir, 0o755); err != nil {
		log.Fatalf("failed to create log directory: %v", err)
	}

	setup(true)

	// Ouvrir le fichier du jour immédiatement.
	mu.Lock()
	now := time.Now()
	rotateIfNeeded(now)
	logPath := filepath.Join(logsDir, now.Format("2006-01-02"), "receiver.log")
	mu.Unlock()

	Info("Logging to %s", logPath)
	return func() {
		mu.Lock()
		defer mu.Unlock()
		if logFile != nil {
			logFile.Close()
			logFile = nil
		}
	}
}

// rotateIfNeeded opens a new log file if the date has changed.
// Must be called with mu held.
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
		fmt.Fprintf(consoleOut, "%s | \033[33mWARN \033[0m Failed to create %s: %v\n",
			now.Format("2006-01-02 15:04:05"), dayDir, err)
		return
	}

	logPath := filepath.Join(dayDir, "receiver.log")
	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		fmt.Fprintf(consoleOut, "%s | \033[33mWARN \033[0m Failed to open %s: %v\n",
			now.Format("2006-01-02 15:04:05"), logPath, err)
		return
	}

	wasRunning := currentDate != ""
	logFile = f
	currentDate = today

	// Notify log rotation at runtime (not on first start,
	// as Init() sends its own message via Info()).
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

