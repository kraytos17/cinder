package log

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/rs/zerolog"
)

func TestParseLevel(t *testing.T) {
	cases := map[string]zerolog.Level{
		"trace": zerolog.TraceLevel,
		"debug": zerolog.DebugLevel,
		"info":  zerolog.InfoLevel,
		"warn":  zerolog.WarnLevel,
		"error": zerolog.ErrorLevel,
	}
	for in, want := range cases {
		got, err := ParseLevel(in)
		if err != nil || got != want {
			t.Fatalf("level %q = %v %v, want %v", in, got, err, want)
		}
	}
	if _, err := ParseLevel("verbose"); err == nil {
		t.Fatal("bad level accepted")
	}
}

func decodeLog(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(buf.Bytes(), &m); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, buf.String())
	}
	return m
}

func TestTraceFieldsCorrelate(t *testing.T) {
	var buf bytes.Buffer
	l := New(&buf, zerolog.TraceLevel)
	Fields(l.Info(), 7, 9).Msg("op")
	m := decodeLog(t, &buf)
	if m["trace_id"] != float64(7) || m["span_id"] != float64(9) {
		t.Fatalf("fields = %v", m)
	}
	if m["message"] != "op" {
		t.Fatalf("message = %v", m["message"])
	}
}

func TestLevelsGate(t *testing.T) {
	var buf bytes.Buffer
	l := New(&buf, zerolog.InfoLevel)
	if e := l.Trace(); e != nil {
		e.Msg("quiet")
	}
	if e := l.Debug(); e != nil {
		e.Msg("quiet")
	}
	if buf.Len() != 0 {
		t.Fatalf("lines emitted above info: %q", buf.String())
	}
	Fields(l.Warn(), 1, 2).Str("opcode", "get").Msg("denied")
	m := decodeLog(t, &buf)
	if m["level"] != "warn" || m["opcode"] != "get" {
		t.Fatalf("warn line = %v", m)
	}
}

func TestNopDrops(t *testing.T) {
	l := Nop()
	l.Info().Msg("nothing")
	ll := l.With().Uint64("trace_id", 1).Logger()
	ll.Error().Msg("nothing")
}
