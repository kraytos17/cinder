package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"gopkg.in/yaml.v3"
)

func checkYAML(ctx context.Context, repo repository, paths []string) error {
	var failures []string
	for _, path := range paths {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		staged, err := stagedBlob(ctx, repo.root, path)
		if err != nil {
			failures = append(failures, err.Error())
			continue
		}

		decoder := yaml.NewDecoder(bytes.NewReader(staged))
		if err := checkYAMLDocuments(decoder); err != nil {
			failures = append(failures, fmt.Sprintf("YAML syntax error in %q: %v", path, err))
			continue
		}
	}
	if len(failures) == 0 {
		return nil
	}
	return fmt.Errorf("YAML check:\n  %s", strings.Join(failures, "\n  "))
}

// checkYAMLDocuments drains every document so multi-document YAML files
// are fully validated, not just their first document.
func checkYAMLDocuments(decoder *yaml.Decoder) error {
	for {
		var value any
		if err := decoder.Decode(&value); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
	}
}
