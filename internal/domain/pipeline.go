package domain

import (
	"fmt"
	"strings"
	"time"
)

type PipelineJobSpec struct {
	Key string `json:"key"`
	Spec
	Dependencies []string `json:"dependencies"`
}
type PipelineSpec struct {
	Jobs []PipelineJobSpec `json:"jobs"`
}
type Pipeline struct {
	CreatedAt  time.Time  `json:"created_at"`
	StartedAt  *time.Time `json:"started_at"`
	FinishedAt *time.Time `json:"finished_at"`
	ID         string     `json:"id"`
	State      string     `json:"state"`
	Jobs       []Job      `json:"jobs"`
}

func (s Spec) WithDefaults() Spec {
	if s.TimeoutSeconds == 0 {
		s.TimeoutSeconds = 30
	}
	if s.MaxAttempts == 0 {
		s.MaxAttempts = 2
	}
	return s
}
func (s PipelineSpec) WithDefaults() PipelineSpec {
	s.Jobs = append([]PipelineJobSpec(nil), s.Jobs...)
	for i := range s.Jobs {
		s.Jobs[i].Spec = s.Jobs[i].Spec.WithDefaults()
	}
	return s
}
func (s PipelineSpec) Validate() error {
	if len(s.Jobs) < 1 || len(s.Jobs) > 128 {
		return fmt.Errorf("pipeline must contain 1..128 jobs")
	}
	jobs := make(map[string]PipelineJobSpec, len(s.Jobs))
	edges := 0
	for _, j := range s.Jobs {
		if strings.TrimSpace(j.Key) == "" || len(j.Key) > 128 || strings.ContainsAny(j.Key, "\x00\r\n") {
			return fmt.Errorf("invalid job key")
		}
		if _, exists := jobs[j.Key]; exists {
			return fmt.Errorf("duplicate job key %q", j.Key)
		}
		if err := j.Spec.Validate(); err != nil {
			return fmt.Errorf("job %q: %w", j.Key, err)
		}
		jobs[j.Key] = j
		edges += len(j.Dependencies)
	}
	if edges > 2048 {
		return fmt.Errorf("pipeline exceeds 2048 dependency edges")
	}
	for _, j := range s.Jobs {
		seen := map[string]bool{}
		for _, parent := range j.Dependencies {
			if parent == j.Key {
				return fmt.Errorf("job %q depends on itself", j.Key)
			}
			if _, ok := jobs[parent]; !ok {
				return fmt.Errorf("job %q: missing dependency %q", j.Key, parent)
			}
			if seen[parent] {
				return fmt.Errorf("job %q: duplicate dependency %q", j.Key, parent)
			}
			seen[parent] = true
		}
	}
	color := map[string]int{}
	var visit func(string) error
	visit = func(key string) error {
		if color[key] == 1 {
			return fmt.Errorf("dependency cycle at %q", key)
		}
		if color[key] == 2 {
			return nil
		}
		color[key] = 1
		for _, parent := range jobs[key].Dependencies {
			if err := visit(parent); err != nil {
				return err
			}
		}
		color[key] = 2
		return nil
	}
	for _, j := range s.Jobs {
		if err := visit(j.Key); err != nil {
			return err
		}
	}
	return nil
}
func TerminalJob(state string) bool {
	return state == "SUCCEEDED" || state == "FAILED" || state == "CANCELLED" || state == "SKIPPED"
}
