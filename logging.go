package goengine

import (
	"fmt"
	"log"
	"os"
	"strings"
)

// LogHandler ...
type LogHandler func(msg string, fields map[string]interface{}, err error)

var logHandler LogHandler

func init() {
	l := log.New(os.Stderr, "[GoEngine] ", log.LstdFlags)

	SetLogHandler(func(msg string, fields map[string]interface{}, err error) {
		if nil != err {
			if nil == fields {
				fields = make(map[string]interface{})
			}
			fields["error"] = err.Error()
		}

		msgParts := make([]string, len(fields)+1)

		msgParts[0] = msg
		idx := 1
		for k, v := range fields {
			msgParts[idx] = fmt.Sprintf("%s=%v", k, v)
			idx++
		}

		l.Println(strings.Join(msgParts, "\t"))
	})
}

// SetLogHandler ...
func SetLogHandler(handler LogHandler) {
	logHandler = handler
}

// Log ...
func Log(msg string, fields map[string]interface{}, err error) {
	logHandler(msg, fields, err)
}
