package main

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestGoBlobNeedsFormat(t *testing.T) {
	formatted := []byte("package foo\n\nfunc Foo() {}\n")
	needsFormat, err := goBlobNeedsFormat(formatted)
	if err != nil {
		t.Fatalf("formatted source returned error: %v", err)
	}
	if needsFormat {
		t.Fatal("formatted source reported as needing format")
	}

	unformatted := []byte("package foo\nfunc Foo(  ){} \n")
	needsFormat, err = goBlobNeedsFormat(unformatted)
	if err != nil {
		t.Fatalf("unformatted source returned error: %v", err)
	}
	if !needsFormat {
		t.Fatal("unformatted source reported as formatted")
	}
	if _, err := goBlobNeedsFormat([]byte("package foo\nfunc (\n")); err == nil {
		t.Fatal("syntax error did not return an error")
	}
}

func TestCheckYAMLDocuments(t *testing.T) {
	valid := "a: 1\n---\nb: [1, 2]\n"
	if err := checkYAMLDocuments(yaml.NewDecoder(strings.NewReader(valid))); err != nil {
		t.Fatalf("valid multi-document YAML failed: %v", err)
	}

	invalid := "a: 1\n---\nb: [unclosed\n"
	if err := checkYAMLDocuments(yaml.NewDecoder(strings.NewReader(invalid))); err == nil {
		t.Fatal("invalid YAML did not return an error")
	}

	empty := ""
	if err := checkYAMLDocuments(yaml.NewDecoder(strings.NewReader(empty))); err != nil {
		t.Fatalf("empty YAML failed: %v", err)
	}
}

func TestClassifyStagedPath(t *testing.T) {
	cases := []struct {
		path               string
		goFile, yaml, just bool
	}{
		{"main.go", true, false, false},
		{"ci/workflow.yml", false, true, false},
		{"ci/workflow.yaml", false, true, false},
		{"Justfile", false, false, true},
		{"tools/Justfile", false, false, false},
		{"README.md", false, false, false},
	}
	for _, tc := range cases {
		goFile, yamlFile, justfile := classifyStagedPath(tc.path)
		if goFile != tc.goFile || yamlFile != tc.yaml || justfile != tc.just {
			t.Errorf("classifyStagedPath(%q) = (%v, %v, %v), want (%v, %v, %v)",
				tc.path, goFile, yamlFile, justfile, tc.goFile, tc.yaml, tc.just)
		}
	}
}
