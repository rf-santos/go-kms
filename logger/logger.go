package logger

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
)

// Level represents the severity level of a log message
type Level int

const (
	// DebugLevel for detailed troubleshooting
	DebugLevel Level = iota
	// InfoLevel for general operational information
	InfoLevel
	// ErrorLevel for error conditions
	ErrorLevel
)

// String returns the string representation of the log level
func (l Level) String() string {
	switch l {
	case DebugLevel:
		return "DEBUG"
	case InfoLevel:
		return "INFO"
	case ErrorLevel:
		return "ERROR"
	default:
		return "UNKNOWN"
	}
}

// ParseLogLevel converts a string to a Level
func ParseLogLevel(level string) Level {
	switch strings.ToLower(level) {
	case "debug":
		return DebugLevel
	case "info":
		return InfoLevel
	case "error":
		return ErrorLevel
	default:
		return InfoLevel // Default to Info level
	}
}

// Logger is a custom logger with level filtering
type Logger struct {
	level  Level
	logger *log.Logger
	mu     sync.Mutex
}

// Global logger instance
var (
	globalLogger *Logger
	once         sync.Once
)

// InitGlobalLogger initializes the global logger with the specified level
func InitGlobalLogger(level Level, output io.Writer) {
	once.Do(func() {
		if output == nil {
			output = os.Stdout
		}
		globalLogger = &Logger{
			level:  level,
			logger: log.New(output, "", log.LstdFlags),
		}
	})
}

// GetLogger returns the global logger instance
func GetLogger() *Logger {
	if globalLogger == nil {
		// Initialize with default settings if not already initialized
		InitGlobalLogger(InfoLevel, os.Stdout)
	}
	return globalLogger
}

// SetLevel sets the log level for the logger
func (l *Logger) SetLevel(level Level) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.level = level
}

// GetLevel returns the current log level
func (l *Logger) GetLevel() Level {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.level
}

// Debug logs a message at Debug level
func (l *Logger) Debug(format string, args ...interface{}) {
	if l.level <= DebugLevel {
		l.mu.Lock()
		defer l.mu.Unlock()
		l.logger.Printf("[DEBUG] "+format, args...)
	}
}

// Info logs a message at Info level
func (l *Logger) Info(format string, args ...interface{}) {
	if l.level <= InfoLevel {
		l.mu.Lock()
		defer l.mu.Unlock()
		l.logger.Printf("[INFO] "+format, args...)
	}
}

// Error logs a message at Error level
func (l *Logger) Error(format string, args ...interface{}) {
	if l.level <= ErrorLevel {
		l.mu.Lock()
		defer l.mu.Unlock()
		l.logger.Printf("[ERROR] "+format, args...)
	}
}

// Fatal logs a message at Error level and then exits
func (l *Logger) Fatal(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.logger.Fatalf("[FATAL] "+format, args...)
}

// Global convenience functions

// Debug logs a message at Debug level using the global logger
func Debug(format string, args ...interface{}) {
	GetLogger().Debug(format, args...)
}

// Info logs a message at Info level using the global logger
func Info(format string, args ...interface{}) {
	GetLogger().Info(format, args...)
}

// Error logs a message at Error level using the global logger
func Error(format string, args ...interface{}) {
	GetLogger().Error(format, args...)
}

// Fatal logs a message at Fatal level using the global logger and then exits
func Fatal(format string, args ...interface{}) {
	GetLogger().Fatal(format, args...)
}

// Printf is a compatibility function for standard logger interface
func Printf(format string, args ...interface{}) {
	Info(format, args...)
}

// Fatalf is a compatibility function for standard logger interface
func Fatalf(format string, args ...interface{}) {
	Fatal(format, args...)
}

// SetGlobalLevel sets the log level for the global logger
func SetGlobalLevel(level Level) {
	GetLogger().SetLevel(level)
}

// GetGlobalLevel returns the current log level of the global logger
func GetGlobalLevel() Level {
	return GetLogger().GetLevel()
}

// FormatLogLevel formats a log level as a string
func FormatLogLevel(level Level) string {
	return fmt.Sprintf("%s", level)
} 