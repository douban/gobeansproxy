package util

import (
	"fmt"
	"path"
	"runtime"
	"testing"
)

func AssertEqual(t *testing.T, a interface{}, b interface{}, message string) {
	if a == b {
		return
	} else {
		_, file, line, ok := runtime.Caller(1)
		if !ok {
			file = "???"
			line = 0
		}
		linepos := fmt.Sprintf("%s:%d", path.Base(file), line)
		valmsg := fmt.Sprintf("%v != %v", a, b)
		t.Fatalf("[%s] %s %s", linepos, valmsg, message)
	}
}
