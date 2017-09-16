package clog

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// Formatter changes the output format of an entry. The key value
// fields in an entry may be output as json, key=value, or any other
// desired format.
type Formatter interface {
	Format(*Entry) ([]byte, error)
}

type textFormatter struct{}

var tag = "pillar"

func (f *textFormatter) Format(entry *Entry) ([]byte, error) {

	b := &bytes.Buffer{}

	combinedFields := make(Fields, len(entry.logger.fields)+len(entry.fields))
	for key, value := range entry.logger.fields {
		combinedFields[key] = value
	}
	for key, value := range entry.fields {
		combinedFields[key] = value
	}

	var keys []string

	for k := range combinedFields {
		if k != "level" && k != "msg" && k != "s" {
			keys = append(keys, k)
		}
	}

	sort.Strings(keys)

	// Always print source second,
	_, hasSource := combinedFields["s"]
	if hasSource {
		s := make([]string, 1)
		s[0] = "s"
		keys = append(s[:], keys...)
	}

	// Always print level first,
	_, hasLevel := combinedFields["level"]
	if hasLevel {
		l := make([]string, 1)
		l[0] = "level"
		keys = append(l[:], keys...)
	}

	// Always print message last,
	_, hasMsg := combinedFields["msg"]
	if hasMsg {
		keys = append(keys, "msg")
	}

	numKeys := len(keys)
	for i, key := range keys {
		value := combinedFields[key]
		if i+1 == numKeys {
			f.appendKeyValue(b, key, value, false)
		} else {
			f.appendKeyValue(b, key, value, true)
		}
	}

	return b.Bytes(), nil
}

const commaSeperator = ", "

func (f *textFormatter) appendKeyValue(b *bytes.Buffer, key, value interface{}, useSeperator bool) {
	seperator := ""
	if useSeperator {
		seperator = commaSeperator
	}

	if key == "level" {
		switch value {
		case ERROR:
			fmt.Fprintf(b, "level=ERROR%s", seperator)
			return
		case WARN:
			fmt.Fprintf(b, "level=WARN%s", seperator)
			return
		case INFO:
			fmt.Fprintf(b, "level=INFO%s", seperator)
			return
		case DEBUG:
			fmt.Fprintf(b, "level=DEBUG%s", seperator)
			return
		}
	}

	valueString := fmt.Sprintf("%v", value)
	if strings.ContainsAny(valueString, " \n,") {
		valueString = strconv.Quote(valueString)
	}
	fmt.Fprintf(b, "%s=%s%s", key, valueString, seperator)

}
