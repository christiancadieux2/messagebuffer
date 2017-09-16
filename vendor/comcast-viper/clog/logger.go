// Package clog implements a simple logging facade that proxies log
// message to the 'standard' go logger.  Debug() and Debugf() messages
// are silently ignored unless EnabledDebug() has been called. It also
// adds the concept of fields, which are key value pieces of
// information that may be sent with a log message. If you wish to add
// new fields to the logger you may use SubLogger for this purpose.
package clog

import "golang.org/x/net/context"

// Logger is the core interface for clog. Loggers have several levels:
// Error, Warn, Info, and Debug. Each of these have an associated
// format string base function as well. NewEntry returns an entry for
// the logger which can specify additional fields to log. SubLogger is
// used to compose a "ChildLogger" which inherits the parents debug
// status and fields.
type Logger struct {
	fields           Fields
	Appender         Appender
	debugEnabled     bool
	silentEnabled    bool
	lineTraceEnabled bool
	goidTraceEnabled bool
}

// Fields are a key value map which may be added to a logger to denote
// contextual information about a log line. One could include a type
// responsible for the log line, or a network source, etc.
type Fields map[string]interface{}

// Level is the log level to send a message at.
type Level uint8

// These are the available log levels for a clog logger.
const (
	// ERROR is for log lines which indicate a serious and
	// possibly unfixable issue.
	ERROR Level = iota
	// WARN is for log lines which indicate that something may be
	// incorrect.
	WARN
	// INFO is for log lines which convely solely informational messages
	// about the state of operation to users.
	INFO
	// DEBUG will not appear unless logger.EnableDebug() is
	// called. Debug messages are intended for engineers debuging
	// code and should not be needed in production.
	DEBUG
)

// MakeLogger constructs a new logger with these associated fields and
// the standard out appender.
func MakeLogger(fields Fields) *Logger {
	return &Logger{
		fields:   fields,
		Appender: standardAppender,
	}
}

// MakeSplunkLogger constructs a new logger with these associated fields and
// a log appender that is compatible with Comcast's enterprise Splunk deployment.
func MakeSplunkLogger(fields Fields) *Logger {
	return &Logger{
		fields:   fields,
		Appender: NewSplunkAppender(standardFormatter),
	}
}

// Fields currently active on this logger. Useful for debug.
func (l *Logger) Fields() Fields {
	return l.fields
}

// SubLogger allocate a new logger that inherits the values of the
// parent logger. New fields overrides fields of the parent when both
// are present.
func (l *Logger) SubLogger(newFields Fields) *Logger {
	f := make(Fields, len(newFields)+len(l.fields))
	for k, v := range l.fields {
		f[k] = v
	}
	for k, v := range newFields {
		f[k] = v
	}
	return &Logger{
		fields:           f,
		Appender:         l.Appender,
		debugEnabled:     l.debugEnabled,
		silentEnabled:    l.silentEnabled,
		lineTraceEnabled: l.lineTraceEnabled,
		goidTraceEnabled: l.goidTraceEnabled,
	}
}

// NewEntry returns an entry which custom fields may be added too.
func (l *Logger) NewEntry() *Entry {
	return &Entry{
		logger: l,
		fields: make(Fields, 2),
	}
}

// KeyError logs a keyed error with the "msg" being err.Error() and
// the "code" value being err.Key(). If a code key is passed in fields
// this will be overridden by the err Key!
// Any provided fields are logged. If multiple fields maps are passed
// all fields are logged. If keys conflict between maps the later key
// trumps.
func (l *Logger) KeyError(err KeyedError, fields ...Fields) {
	f := Fields{}
	for _, field := range fields {
		for k, v := range field {
			f[k] = v
		}
	}
	f["code"] = err.Key()
	l.NewEntry().WithFields(f).Print(ERROR, err.Error())
}

// Error logs v to this logger with level ERROR.
func (l *Logger) Error(v ...interface{}) {
	l.NewEntry().Print(ERROR, v...)

}

// Errorf logs format & v to this logger with level ERROR.
func (l *Logger) Errorf(format string, v ...interface{}) {
	l.NewEntry().Printf(ERROR, format, v...)
}

// ErrorfFields logs an entry with fields f, format & v to this logger
// with level ERROR.
func (l *Logger) ErrorfFields(f Fields, format string, v ...interface{}) {
	l.NewEntry().WithFields(f).Printf(ERROR, format, v...)
}

// Warn logs v to this logger with level WARN.
func (l *Logger) Warn(v ...interface{}) {
	l.NewEntry().Print(WARN, v...)
}

// Warnf logs format & v to this logger with level WARN.
func (l *Logger) Warnf(format string, v ...interface{}) {
	l.NewEntry().Printf(WARN, format, v...)
}

// WarnfFields logs an entry with fields f, format & v to this logger
// with level WARN.
func (l *Logger) WarnfFields(f Fields, format string, v ...interface{}) {
	l.NewEntry().WithFields(f).Printf(WARN, format, v...)
}

// Info logs v to this logger with level INFO.
func (l *Logger) Info(v ...interface{}) {
	l.NewEntry().Print(INFO, v...)
}

// Infof logs format & v to this logger with level INFO.
func (l *Logger) Infof(format string, v ...interface{}) {
	l.NewEntry().Printf(INFO, format, v...)
}

// InfofFields logs an entry with fields f, format & v to this logger
// with level INFO.
func (l *Logger) InfofFields(f Fields, format string, v ...interface{}) {
	l.NewEntry().WithFields(f).Printf(INFO, format, v...)
}

// Debug logs v to this logger with level DEBUG, if EnableDebug()
// has been called.
func (l *Logger) Debug(v ...interface{}) {
	if l.debugEnabled {
		l.NewEntry().Print(DEBUG, v...)
	}
}

// Debugf logs format & v to this logger with level DEBUG, if EnableDebug()
// has been called.
func (l *Logger) Debugf(format string, v ...interface{}) {
	if l.debugEnabled {
		l.NewEntry().Printf(DEBUG, format, v...)
	}
}

// DebugfFields logs an entry with fields f, format & v to this logger
// with level DEBUG if EnableDebug() has been called.
func (l *Logger) DebugfFields(f Fields, format string, v ...interface{}) {
	if l.debugEnabled {
		l.NewEntry().WithFields(f).Printf(DEBUG, format, v...)
	}
}

func (l *Logger) write(entry *Entry) {
	l.Appender.Append(entry)
}

// EnableDebug logs to be written to this logger.
func (l *Logger) EnableDebug() {
	l.debugEnabled = true
}

// EnableLineTrace enables line trace info to be written to this logger.
func (l *Logger) EnableLineTrace() {
	l.lineTraceEnabled = true
}

// EnableGoidTrace enables goroutine id trace info to be written to this logger.
// This is NOT RECOMMENDED FOR PRODUCTION CODE
func (l *Logger) EnableGoidTrace() {
	l.goidTraceEnabled = true
}

// EnableSilent supresses all log output
func (l *Logger) EnableSilent() {
	l.silentEnabled = true
}

type key int

const (
	loggerKey key = iota
	tracerKey
)

// NewLogContext with a logger.
func NewLogContext(ctx context.Context, l *Logger) context.Context {
	return context.WithValue(ctx, loggerKey, l)
}

// CtxLogger returns the logger for the context or nil and false if
// not present.
func CtxLogger(ctx context.Context) (*Logger, bool) {
	l, ok := ctx.Value(loggerKey).(*Logger)
	return l, ok
}

// Return the context logger or the default logger if not present.
func CtxLogDef(ctx context.Context) *Logger {
	l, ok := CtxLogger(ctx)
	if !ok {
		return StandardLogger()
	}
	return l

}

// TraceEntry populated with the values of the trace.
func (l *Logger) TraceEntry(t *Trace) *Entry {
	e := l.NewEntry().WithFields(Fields{
		"trace-id":      t.TraceID,
		"span-id":       t.SpanID,
		"parent-id":     t.ParentID,
		"span-name":     t.SpanName,
		"span-duration": t.Duration.Nanoseconds() / 1000, // To micros
	}).WithFields(l.fields)
	if t.Err != nil {
		e = e.WithFields(Fields{
			"error-code":   t.Err.Error(),
			"span-success": false,
		})
	} else {
		e = e.WithFields(Fields{
			"span-success": true,
		})
	}
	if t.HTTPStatus != 0 {
		e = e.WithFields(Fields{
			"response-status": t.HTTPStatus,
		})
		if t.ResponseDuration != 0 {
			e = e.WithFields(Fields{
				"response-duration": t.ResponseDuration.Nanoseconds() / 1000, // To micros
			})
		}
		if t.StatusDuration != 0 {
			e = e.WithFields(Fields{
				"status-duration": t.StatusDuration.Nanoseconds() / 1000, // To micros
			})
		}
	}
	return e
}
