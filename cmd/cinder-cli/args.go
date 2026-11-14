package main

import (
	"errors"
	"slices"
	"strings"
)

type keyValuePairs struct {
	keys, values []string
}

func parseKeyValuePairs(args []string) (keyValuePairs, error) {
	if len(args) == 0 || len(args)%2 != 0 {
		return keyValuePairs{}, errors.New("mset requires key/value pairs")
	}

	out := keyValuePairs{
		keys:   make([]string, 0, len(args)/2),
		values: make([]string, 0, len(args)/2),
	}
	for kv := range slices.Chunk(args, 2) {
		out.keys = append(out.keys, kv[0])
		out.values = append(out.values, kv[1])
	}
	return out, nil
}

// extractTTL preserves the established `mset KEY VALUE --ttl MS` spelling,
// since command flags otherwise parse before positional arguments.
func extractTTL(args []string) ([]string, string, error) {
	// Output never exceeds input: presize to skip growth reallocs.
	out := make([]string, 0, len(args))
	ttl := ""
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--ttl" {
			if i+1 >= len(args) {
				return nil, "", errors.New("flag --ttl requires an argument")
			}

			ttl = args[i+1]
			i++
			continue
		}
		if v, ok := strings.CutPrefix(a, "--ttl="); ok {
			ttl = v
			continue
		}
		out = append(out, a)
	}
	return out, ttl, nil
}
