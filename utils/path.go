package utils

import (
	"path"
	"runtime"
)

func GetProjectHomeDir() string {
	if _, filename, _, ok := runtime.Caller(1); ok {
		return path.Dir(path.Dir(filename))
	}
	return ""
}
