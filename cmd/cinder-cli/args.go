package main

import (
	"errors"
	"fmt"
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
	for i := 0; i < len(args); i += 2 {
		out.keys = append(out.keys, args[i])
		out.values = append(out.values, args[i+1])
	}
	return out, nil
}

// extractTTL preserves the established `mset KEY VALUE --ttl MS` spelling,
// since command flags otherwise parse before positional arguments.
func extractTTL(args []string) ([]string, string, error) {
	var out []string
	ttl := ""
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--ttl" {
			if i+1 >= len(args) {
				return nil, "", fmt.Errorf("flag --ttl requires an argument")
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
