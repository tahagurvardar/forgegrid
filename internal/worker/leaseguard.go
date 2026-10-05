package worker

import "time"

// Request time includes a monotonic clock. Starting at ACK receipt would give a
// delayed ACK extra authority beyond the database lease. Never extend from send alone.
func LeaseDeadline(sent time.Time, ttl time.Duration) time.Time {
	return sent.Add(ttl - 100*time.Millisecond)
}
func LeaseValid(deadline, now time.Time) bool { return !deadline.IsZero() && now.Before(deadline) }
