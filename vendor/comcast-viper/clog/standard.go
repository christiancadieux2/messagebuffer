package clog

import golog "log"

var standardFormatter = &textFormatter{}

// Global 'standardAppender' can be changed by calling EnableSyslogFormat()
var standardAppender Appender = &logAppender{
	formatter: standardFormatter,
}

var standardLogger = &Logger{
	fields:   Fields{},
	Appender: standardAppender,
}

// Error logs v to the standard logger with level ERROR.
func Error(v ...interface{}) {
	standardLogger.Error(v...)
}

// Errorf logs format & v to the standard logger with level ERROR.
func Errorf(format string, v ...interface{}) {
	standardLogger.Errorf(format, v...)
}

// ErrorfFields logs an entry with fields f, format & v to the standard
// logger with level ERROR.
func ErrorfFields(f Fields, format string, v ...interface{}) {
	standardLogger.ErrorfFields(f, format, v...)
}

// Warn logs v to the standard logger with level WARN.
func Warn(v ...interface{}) {
	standardLogger.Warn(v...)
}

// Warnf logs format & v to the standard logger with level WARN.
func Warnf(format string, v ...interface{}) {
	standardLogger.Warnf(format, v...)
}

// WarnfFields logs an entry with fields f, format & v to the standard
// logger with level WARN.
func WarnfFields(f Fields, format string, v ...interface{}) {
	standardLogger.WarnfFields(f, format, v...)
}

// Info logs v to the standard logger with level INFO.
func Info(v ...interface{}) {
	standardLogger.Info(v...)
}

// Infof logs format & v to the standard logger with level INFO.
func Infof(format string, v ...interface{}) {
	standardLogger.Infof(format, v...)
}

// InfofFields logs an entry with fields f, format & v to the standard
// logger with level INFO.
func InfofFields(f Fields, format string, v ...interface{}) {
	standardLogger.InfofFields(f, format, v...)
}

// Debug logs v to the standard logger with level DEBUG, if EnableDebug()
// has been called.
func Debug(v ...interface{}) {
	standardLogger.Debug(v...)
}

// Debugf logs format & v to the standard logger with level DEBUG, if EnableDebug()
// has been called.
func Debugf(format string, v ...interface{}) {
	standardLogger.Debugf(format, v...)
}

// DebugfFields logs an entry with fields f, format & v to the standard
// logger with level DEBUG.
func DebugfFields(f Fields, format string, v ...interface{}) {
	standardLogger.DebugfFields(f, format, v...)
}

// NewEntry creates a new LogEntry on the standard logger.
func NewEntry() *Entry {
	return &Entry{
		logger: standardLogger,
		fields: make(Fields, 2),
	}
}

// EnableDebug logs to be written to the standard logger.
func EnableDebug() {
	standardLogger.debugEnabled = true
}

// EnableLineTrace enables line trace to be written to the standard logger.
func EnableGoidTrace() {
	standardLogger.goidTraceEnabled = true
}

// EnableLineTrace enables line trace to be written to the standard logger.
func EnableLineTrace() {
	standardLogger.lineTraceEnabled = true
}

// EnableSilent suppresses all log output.
func EnableSilent() {
	standardLogger.silentEnabled = true
}

// EnableSyslogFormat on standard logger.
func EnableSyslogFormat() {
	golog.SetFlags(0) // supress golog generated timestamp prefix
	syslogFormatAppender := &syslogAppender{
		formatter: standardFormatter,
	}
	standardAppender = syslogFormatAppender
	standardLogger.Appender = syslogFormatAppender
}

// StandardLogger is usefull for base inheritance or base level (no
// fields) logging.
func StandardLogger() *Logger {
	return standardLogger
}
