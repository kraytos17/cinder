package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
)

func stagedPaths(ctx context.Context, root string) ([]string, error) {
	cmd := exec.CommandContext(ctx, "git", "diff", "--cached", "--name-only", "-z", "--diff-filter=ACMR")
	cmd.Dir = root
	out, err := cmd.Output()
	if err != nil {
		if exitErr, ok := errors.AsType[*exec.ExitError](err); ok {
			return nil, fmt.Errorf("list staged paths: %s: %w", strings.TrimSpace(string(exitErr.Stderr)), err)
		}
		return nil, fmt.Errorf("list staged paths: %w", err)
	}
	if len(out) == 0 {
		return nil, nil
	}
	return strings.Split(strings.TrimSuffix(string(out), "\x00"), "\x00"), nil
}

func stagedBlob(ctx context.Context, root, path string) ([]byte, error) {
	//nolint:gosec // G204: path comes from the local git index listing, not remote input.
	cmd := exec.CommandContext(ctx, "git", "show", ":"+path)
	cmd.Dir = root

	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("read staged blob %q: %s: %w", path, strings.TrimSpace(stderr.String()), err)
	}
	return out, nil
}

func checkWhitespace(ctx context.Context, root string) error {
	cmd := exec.CommandContext(ctx, "git", "diff", "--cached", "--check", "--", ".", ":!*.md")
	cmd.Dir = root
	out, err := cmd.CombinedOutput()
	if err == nil {
		return nil
	}

	message := strings.TrimSpace(string(out))
	if message == "" {
		message = err.Error()
	}
	return fmt.Errorf("staged whitespace errors:\n%s", message)
}

func hasUnstagedChanges(ctx context.Context, root, path string) (bool, error) {
	//nolint:gosec // G204: path comes from the local git index listing, not remote input.
	cmd := exec.CommandContext(ctx, "git", "diff", "--quiet", "--", path)
	cmd.Dir = root
	err := cmd.Run()
	if err == nil {
		return false, nil
	}
	if exitErr, ok := errors.AsType[*exec.ExitError](err); ok && exitErr.ExitCode() == 1 {
		return true, nil
	}
	return false, fmt.Errorf("check unstaged changes for %q: %w", path, err)
}
