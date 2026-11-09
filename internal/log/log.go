// Package log builds the process logger: structured, leveled, JSON on
// stderr. Levels mirror the C++ --log-level flag
// (trace|debug|info|warn|error, default info), all native to zerolog.
//
// Distributed correlation comes from trace_id/span_id fields: dispatch
// attaches the request's wire IDs to a child logger, so every line in a
// request's path — including cross-node fan-out, which forwards the IDs
// — carries them, and external aggregation (journald/files → Loki/ELK,
// out of scope) reconstructs the path with trace_id=.... No log shipping
// between nodes is built or planned.
package log

import (
	"fmt"
	"io"
	"os"

	"github.com/rs/zerolog"
)

// ParseLevel maps the --log-level flag to a zerolog level.
func ParseLevel(s string) (zerolog.Level, error) {
	switch s {
	case "trace":
		return zerolog.TraceLevel, nil
	case "debug":
		return zerolog.DebugLevel, nil
	case "info":
		return zerolog.InfoLevel, nil
	case "warn":
		return zerolog.WarnLevel, nil
	case "error":
		return zerolog.ErrorLevel, nil
	default:
		return zerolog.InfoLevel, fmt.Errorf("log: bad level %q, want trace|debug|info|warn|error", s)
	}
}

// New returns a JSON logger on w (stderr in production) at the given
// level. Caller owns Sync via the returned closer is unnecessary:
// zerolog writes synchronously, nothing buffers.
func New(w io.Writer, level zerolog.Level) zerolog.Logger {
	if w == nil {
		w = os.Stderr
	}
	return zerolog.New(w).Level(level).With().Timestamp().Logger()
}

// Fields appends the distributed-correlation fields (the wire trace/span
// IDs the request already carries) to an enabled event. Attach once at
// the dispatch entry; fan-out forwards the IDs, so replica-side lines
// correlate. The event must be non-nil: gate with `if e := l.Debug(); e
// != nil` first, since disabled levels yield nil events.
func Fields(e *zerolog.Event, traceID, spanID uint64) *zerolog.Event {
	return e.Uint64("trace_id", traceID).Uint64("span_id", spanID)
}

var nop = zerolog.New(io.Discard).Level(zerolog.Disabled)

// Nop returns a disabled logger for tests and unconfigured servers.
func Nop() zerolog.Logger { return nop }
