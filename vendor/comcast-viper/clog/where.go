package clog

import (
	"fmt"
	"runtime"
	"strings"
)

var depth int

// IndentWidth defaults to 0 (no indentation) and can be changed to cause
// the formatted log entries to be indented. Recommended value 4
var IndentWidth int // off by default

// Indent returns a string of spaces IndentWidth * depth long
func Indent() (res string) {
	istr := "                                                            " +
		"                                                            "
	n := depth * IndentWidth
	if n < 0 {
		return "<<<<<<<"
	}
	return istr[0:n]
}

// EnterStr formats a string with the (function/line number)showing entry to a function
func EnterStr() (res string) {
	ptr, _, line, ok := runtime.Caller(1)
	if !ok {
		return "Enter:Unk()"
	}
	f := runtime.FuncForPC(ptr)
	// remove everything up to the last '/'
	name := f.Name()
	name = name[strings.LastIndex(name, "/")+1:]
	res = fmt.Sprintf("%s{ Enter:%s:%d: ", Indent(), name, line)
	depth++
	return //implied
}

// ExitStr formats a string with the (function/line number) showing exit to a function
func ExitStr() (res string) {
	ptr, _, line, ok := runtime.Caller(1)
	if !ok {
		return "Exit:Unk()"
	}
	f := runtime.FuncForPC(ptr)
	// remove everything up to the last '/'
	name := f.Name()
	name = name[strings.LastIndex(name, "/")+1:]
	depth--
	res = fmt.Sprintf("%s} Exit:%s:%d: ", Indent(), name, line)
	return //implied
}

// WhereStr formats a string with the 'where' data (file:line)
func WhereStr() string {
	_, file, line, ok := runtime.Caller(1)
	if ok {
		// Truncate file name at last file name separator.
		if index := strings.LastIndex(file, "/"); index >= 0 {
			file = file[index+1:]
		} else if index = strings.LastIndex(file, "\\"); index >= 0 {
			file = file[index+1:]
		}
	} else {
		file = "???"
		line = 1
	}
	return fmt.Sprintf("%s>>%s:%d: ", Indent(), file, line)
}

// WhereStr formats a string with the 'where' data (file:line)
func FuncStr() string {
	var funcstr string
	pc, _, line, ok := runtime.Caller(1)
	if ok {
		funcstr = runtime.FuncForPC(pc).Name()
		if index := strings.LastIndex(funcstr, "/"); index >= 0 {
			funcstr = funcstr[index+1:]
		}
	} else {
		funcstr = "function name lookup failed"
		line = 99999
	}
	return fmt.Sprintf("%s>>%s:%d: ", Indent(), funcstr, line)
}

// BTrace returns a slice of strings showing the stack backtrace
func BTrace() (res []string) {
	res = append(res, WhereStr())
	for i := 1; ; i++ {
		ptr, _, _, ok := runtime.Caller(i)
		if ok {
			f := runtime.FuncForPC(ptr)
			name := f.Name()
			// remove everything up to the last '/'
			name = name[strings.LastIndex(name, "/")+1:]
			res = append(res, name)
		} else {
			res = append(res, "END")
			break
		}
	}
	return // implied
}
