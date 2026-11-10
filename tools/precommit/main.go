package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "pre-commit:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	repo, err := findRepository(ctx)
	if err != nil {
		return err
	}

	var failures []string
	paths, err := stagedPaths(ctx, repo.root)
	if err != nil {
		return err
	}
	if err := checkWhitespace(ctx, repo.root); err != nil {
		failures = append(failures, err.Error())
	}

	var goFiles, yamlFiles []string
	checkJustfile := false
	for _, path := range paths {
		goFile, yamlFile, justfile := classifyStagedPath(path)
		if goFile {
			goFiles = append(goFiles, path)
		}
		if yamlFile {
			yamlFiles = append(yamlFiles, path)
		}
		if justfile {
			checkJustfile = true
		}
	}
	if err := checkGoFormat(ctx, repo, goFiles); err != nil {
		failures = append(failures, err.Error())
	}
	if err := checkYAML(ctx, repo, yamlFiles); err != nil {
		failures = append(failures, err.Error())
	}
	if checkJustfile {
		if err := checkJustfileFormat(ctx, repo); err != nil {
			failures = append(failures, err.Error())
		}
	}
	if len(failures) > 0 {
		return fmt.Errorf("checks failed:\n  %s", strings.Join(failures, "\n  "))
	}

	fmt.Fprintln(os.Stdout, "pre-commit: OK (fast staged-content checks).")
	fmt.Fprintln(os.Stdout, "  Remember to run: just verify")
	return nil
}

type repository struct {
	root string
}

// classifyStagedPath reports which checks a staged path participates in.
// Only a top-level Justfile triggers the Justfile check; nested files
// named Justfile belong to other tools and are ignored.
func classifyStagedPath(path string) (goFile, yamlFile, justfile bool) {
	return strings.HasSuffix(path, ".go"),
		strings.HasSuffix(path, ".yml") || strings.HasSuffix(path, ".yaml"),
		filepath.Clean(path) == "Justfile"
}

func findRepository(ctx context.Context) (repository, error) {
	out, err := commandOutput(ctx, "git", "rev-parse", "--show-toplevel")
	if err != nil {
		return repository{}, fmt.Errorf("find repository root: %w", err)
	}

	root := strings.TrimSpace(string(out))
	if root == "" {
		return repository{}, fmt.Errorf("git returned an empty repository root")
	}
	return repository{root: root}, nil
}

func commandOutput(ctx context.Context, name string, args ...string) ([]byte, error) {
	//nolint:gosec // G204: invoked with fixed git subcommands, not remote input.
	cmd := exec.CommandContext(ctx, name, args...)
	out, err := cmd.Output()
	if err != nil {
		if exitErr, ok := errors.AsType[*exec.ExitError](err); ok {
			return nil, fmt.Errorf("%s %s: %s: %w", name, strings.Join(args, " "), strings.TrimSpace(string(exitErr.Stderr)), err)
		}
		return nil, fmt.Errorf("%s %s: %w", name, strings.Join(args, " "), err)
	}
	return out, nil
}
