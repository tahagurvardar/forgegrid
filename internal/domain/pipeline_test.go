package domain

import "testing"

func TestStaticDAGValidation(t *testing.T) {
	job := func(key string, deps ...string) PipelineJobSpec {
		return PipelineJobSpec{Key: key, Spec: Spec{Image: "alpine", Command: []string{"true"}, TimeoutSeconds: 30, MaxAttempts: 2}, Dependencies: deps}
	}
	for _, c := range []struct {
		name  string
		jobs  []PipelineJobSpec
		valid bool
	}{
		{"single", []PipelineJobSpec{job("one")}, true},
		{"diamond", []PipelineJobSpec{job("package", "test", "lint"), job("test", "build"), job("lint", "build"), job("build")}, true},
		{"empty", nil, false},
		{"duplicate", []PipelineJobSpec{job("a"), job("a")}, false},
		{"self", []PipelineJobSpec{job("a", "a")}, false},
		{"missing", []PipelineJobSpec{job("a", "b")}, false},
		{"cycle", []PipelineJobSpec{job("a", "b"), job("b", "c"), job("c", "a")}, false},
		{"duplicate-edge", []PipelineJobSpec{job("a"), job("b", "a", "a")}, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			err := (PipelineSpec{Jobs: c.jobs}).Validate()
			if (err == nil) != c.valid {
				t.Fatalf("valid=%v err=%v", c.valid, err)
			}
		})
	}
}
