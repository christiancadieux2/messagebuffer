package clog

import (
	"fmt"
	golog "log"
	"log/syslog"
	"os"
	"time"
)

// Appender represents an "outlet" for logs to be sent to. Examples
// include syslog and standard out. If appending failed it should
// return an error describing the reason.
type Appender interface {
	Append(*Entry) error
}

type logAppender struct {
	formatter Formatter
}

// SplunkAppender creates a log string that is compatible with the Comcast enterprise Splunk deployment.
// Specifically, it formats a log string of the form "yyyy-mm-dd hh:mm:ss.xxx <some log message"".
type SplunkAppender struct {
	formatter Formatter
	logger    *golog.Logger
}

type syslogAppender struct {
	formatter Formatter
}

func (a *logAppender) Append(entry *Entry) error {
	formatted, err := a.formatter.Format(entry)
	if err != nil {
		return err
	}
	golog.Print(string(formatted))
	return nil
}

// Append emits a Comcast Splunk compatible log message.
func (a *SplunkAppender) Append(entry *Entry) error {
	fmtNow := time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
	formatted, err := a.formatter.Format(entry)
	if err != nil {
		return err
	}
	msg := fmt.Sprintf("%s %s\r\n", fmtNow, string(formatted))
	a.logger.Print(msg)
	return nil
}

func (a *syslogAppender) Append(entry *Entry) error {
	formatted, err := a.formatter.Format(entry)
	if err != nil {
		return err
	}
	timestamp := time.Now().Format(time.StampMilli)
	golog.Printf("<%d>%s %s[%d]: %s", priority(entry), timestamp, tag, os.Getpid(), string(formatted))
	return nil
}

// NewSplunkAppender creates a new SplunkAppender instance.
func NewSplunkAppender(formatter Formatter) *SplunkAppender {
	return &SplunkAppender{
		formatter: formatter,
		logger:    golog.New(os.Stderr, "", 0),
	}
}

var syslogFacilityMask = syslog.LOG_LOCAL3 << 3 // local_3 * 8
//var syslogFacilityMask = syslog.LOG_KERN << 3 // local_3 * 8

func priority(entry *Entry) syslog.Priority {
	level, ok := entry.fields["level"]
	if ok {
		switch level {
		case ERROR:
			return syslogFacilityMask | syslog.LOG_ERR
		case WARN:
			return syslogFacilityMask | syslog.LOG_WARNING
		case INFO:
			return syslogFacilityMask | syslog.LOG_INFO
		case DEBUG:
			return syslogFacilityMask | syslog.LOG_DEBUG
		}
	}

	return syslogFacilityMask | syslog.LOG_INFO
}
