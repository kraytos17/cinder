package main

import (
	"bytes"
	"context"
	"fmt"
	"go/format"
	"strings"
)

func checkGoFormat(ctx context.Context, repo repository, paths []string) error {
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

		needsFormat, formatErr := goBlobNeedsFormat(staged)
		if formatErr != nil {
			failures = append(failures, fmt.Sprintf("format staged Go file %q: %v", path, formatErr))
			continue
		}
		if !needsFormat {
			continue
		}

		failure := fmt.Sprintf("%s is not gofmt-formatted; run gofmt -w %q and re-stage", path, path)
		unstaged, err := hasUnstagedChanges(ctx, repo.root, path)
		if err != nil {
			failures = append(failures, err.Error())
			continue
		}
		if unstaged {
			failure += " (unstaged changes exist; check partial staging before formatting)"
		}
		failures = append(failures, failure)
	}
	if len(failures) == 0 {
		return nil
	}
	return fmt.Errorf("gofmt check:\n  %s", strings.Join(failures, "\n  "))
}

// goBlobNeedsFormat reports whether src differs from gofmt output.
func goBlobNeedsFormat(src []byte) (bool, error) {
	formatted, err := format.Source(src)
	if err != nil {
		return false, err
	}
	return !bytes.Equal(src, formatted), nil
}
