package main

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"mvdan.cc/gofumpt/format"
)

const (
	gofumptLangVersion = "go1.27"
	gofumptModulePath  = "github.com/kraytos17/cinder"
)

func checkGoFumpt(ctx context.Context, repo repository, paths []string) error {
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

		needsFormat, formatErr := goBlobNeedsGofumpt(staged)
		if formatErr != nil {
			failures = append(failures, fmt.Sprintf("format staged Go file %q: %v", path, formatErr))
			continue
		}
		if !needsFormat {
			continue
		}

		failure := fmt.Sprintf("%s is not gofumpt-formatted; run gofumpt -w %q (or just lint-fix) and re-stage", path, path)
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
	return fmt.Errorf("gofumpt check:\n  %s", strings.Join(failures, "\n  "))
}

// goBlobNeedsGofumpt reports whether src differs from gofumpt output.
// gofumpt is a strict superset of gofmt, matching the CI lint gate.
func goBlobNeedsGofumpt(src []byte) (bool, error) {
	formatted, err := format.Source(src, format.Options{
		LangVersion: gofumptLangVersion,
		ModulePath:  gofumptModulePath,
	})
	if err != nil {
		return false, err
	}
	return !bytes.Equal(src, formatted), nil
}
