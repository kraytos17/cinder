package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

func checkJustfileFormat(ctx context.Context, repo repository) error {
	staged, err := stagedBlob(ctx, repo.root, "Justfile")
	if err != nil {
		return err
	}

	temp, err := os.CreateTemp("", "cinder-staged-Justfile-*")
	if err != nil {
		return fmt.Errorf("create temporary Justfile: %w", err)
	}

	path := temp.Name()
	defer os.Remove(path)
	if _, err := temp.Write(staged); err != nil {
		temp.Close()
		return fmt.Errorf("write temporary Justfile: %w", err)
	}
	if err := temp.Close(); err != nil {
		return fmt.Errorf("close temporary Justfile: %w", err)
	}

	//nolint:gosec // G204: justfile path is a local temp file created above, not remote input.
	cmd := exec.CommandContext(ctx, "just", "--fmt", "--check", "--justfile", path)
	cmd.Dir = repo.root
	if output, err := cmd.CombinedOutput(); err != nil {
		unstaged, diffErr := hasUnstagedChanges(ctx, repo.root, "Justfile")
		if diffErr != nil {
			return diffErr
		}

		detail := strings.TrimSpace(string(output))
		message := "staged Justfile is not formatted; run just --fmt and re-stage"
		if unstaged {
			message += " (unstaged changes exist; check partial staging before formatting)"
		}
		if detail != "" {
			message += ": " + detail
		}
		return fmt.Errorf("justfile check: %s", message)
	}
	return nil
}
