package clog

import (
	"fmt"
	"path/filepath"
	"runtime"
	"strconv"
        "strings"
)

// Entry represents an individual log entry.
type Entry struct {
	logger *Logger
	fields Fields
}

// KeyedError adds the additional key method to allow for for
// automatic printing of a key when printing an entry. This is usefull
// for adding an error type with unique error keys (codes).
type KeyedError interface {
	error
	Key() string
}

// WithField adds a field to this entry to give it context, overriding
// any existing field.
func (entry *Entry) WithField(key string, value interface{}) *Entry {
	return entry.WithFields(Fields{key: value})
}

// WithFields adds all fields in fields to this entry. Any existing
// field will be overridden.
func (entry *Entry) WithFields(fields Fields) *Entry {
	combinedFields := make(Fields, len(entry.fields)+len(fields))

	for key, value := range entry.fields {
		combinedFields[key] = value
	}
	for key, value := range fields {
		combinedFields[key] = value
	}
	return &Entry{
		logger: entry.logger,
		fields: combinedFields,
	}
}

func addTraceInfo(entry *Entry) {
	if entry.logger.lineTraceEnabled {
		_, fn, line, _ := runtime.Caller(4)
		_, fileName := filepath.Split(fn)
		entry.fields["line"] = fileName + ":" + strconv.Itoa(line)
	}
        if entry.logger.goidTraceEnabled {
            entry.fields["goid"] = goid()
        }
}

// This expects behavior from the runtime.Stack function that may not be guaranteed
// for all future versions, so, this should definitely NOT BE USED for production code.
func goid() string {
    var buf [64]byte
    n := runtime.Stack(buf[:], false)
    idField := strings.Fields(strings.TrimPrefix(string(buf[:n]), "goroutine "))[0]
    id, err := strconv.Atoi(idField)
    if err != nil {
        return "BAD GOID"
    }
    if id == 0 {
        return "BAD GOID (0)"
    }
    return strconv.Itoa(id)
}

// Print writes the given list of interfaces to the logger to the
// entrie's logger.
func (entry *Entry) Print(level Level, v ...interface{}) {
	if (level == DEBUG && !entry.logger.debugEnabled) || entry.logger.silentEnabled {
		return
	}
	entry.fields["msg"] = fmt.Sprint(v...)
	addTraceInfo(entry)
	entry.populateEntries(level, v...)
	entry.logger.write(entry)
}

// Printf writes a message with the given format string and interfaces
// to the entrie's logger.
func (entry *Entry) Printf(level Level, format string, v ...interface{}) {
	if (level == DEBUG && !entry.logger.debugEnabled) || entry.logger.silentEnabled {
		return
	}
	entry.fields["msg"] = fmt.Sprintf(format, v...)
	addTraceInfo(entry)
	entry.populateEntries(level, v...)

	entry.logger.write(entry)
}

func (entry *Entry) populateEntries(level Level, v ...interface{}) {
	entry.fields["level"] = level

	for _, obj := range v {
		if err, ok := obj.(KeyedError); ok {
			entry.fields["code"] = err.Key()
			entry.fields["msg"] = err.Error()
		}
	}
}

// Write the entry to it's logger without any message.
func (entry *Entry) Write(level Level) {
	if (level == DEBUG && !entry.logger.debugEnabled) || entry.logger.silentEnabled {
		return
	}
	entry.fields["level"] = level

	entry.logger.write(entry)

}
