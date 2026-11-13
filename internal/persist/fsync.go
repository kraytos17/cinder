package persist

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// ParseFsyncPolicy parses "none", "always", or "interval:<milliseconds>".
func ParseFsyncPolicy(raw string) (FsyncMode, time.Duration, error) {
	switch raw {
	case "", "none":
		return FsyncNone, 0, nil
	case "always":
		return FsyncAlways, 0, nil
	}

	value, ok := strings.CutPrefix(raw, "interval:")
	if !ok {
		return 0, 0, fmt.Errorf("--fsync must be none, always, or interval:<ms>")
	}

	ms, err := strconv.ParseInt(value, 10, 64)
	if err != nil || ms <= 0 || ms > int64(time.Duration(1<<63-1)/time.Millisecond) {
		return 0, 0, fmt.Errorf("--fsync interval must be a positive number of milliseconds")
	}
	return FsyncInterval, time.Duration(ms) * time.Millisecond, nil
}
