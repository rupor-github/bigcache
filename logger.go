package bigcache

import (
	"log"
	"os"
)

// Logger interface.
type Logger interface {
	Printf(format string, v ...interface{})
}

// this is a safeguard, breaking on compile time in case log.Logger does not implement Logger interface.
var _ Logger = &log.Logger{}

type nopLogger struct{}

func (*nopLogger) Printf(_ string, _ ...interface{}) {}

// NopLogger returns `empty` logger with no output.
func newNopLogger() Logger {
	return &nopLogger{}
}

// DefaultLogger returns implementation backed by stdlib's log.
func DefaultLogger() Logger {
	return log.New(os.Stdout, "", log.LstdFlags)
}
