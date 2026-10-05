package controlplane

import (
	pb "forgegrid/gen/go/forgegrid/v1"
	"testing"
	"time"
)

func TestRegisteredQueuedBeforeSessionPublication(t *testing.T) {
	s := New(nil, 2*time.Second)
	out := s.attach("session")
	// This models a scheduler dispatching as soon as the session becomes visible.
	if !s.send(s.connected()[0], &pb.ControlMessage{Body: &pb.ControlMessage_RunAttempt{RunAttempt: &pb.RunAttempt{JobId: "job"}}}) {
		t.Fatal("dispatch failed")
	}
	if first := <-out; first.GetRegistered() == nil {
		t.Fatal("assignment preceded registration ACK")
	}
	if second := <-out; second.GetRunAttempt() == nil {
		t.Fatal("assignment missing")
	}
}
