package worker

import (
	"testing"
	"time"
)

func TestDelayedACKDoesNotExtendAuthority(t *testing.T) {
	sent := time.Now()
	deadline := LeaseDeadline(sent, 10*time.Second)
	if LeaseValid(deadline, sent.Add(10*time.Second)) {
		t.Fatal("delayed ACK extends expired authority")
	}
	if !LeaseValid(deadline, sent.Add(9*time.Second)) {
		t.Fatal("valid ACK rejected")
	}
	if LeaseValid(time.Time{}, sent) {
		t.Fatal("request without ACK granted authority")
	}
	if LeaseValid(deadline, deadline) {
		t.Fatal("deadline boundary valid")
	}
}
