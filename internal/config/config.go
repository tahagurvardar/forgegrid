package config

import (
	"fmt"
	"os"
	"time"
)

func Env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
func Duration(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil || d < 100*time.Millisecond || d > time.Hour {
		panic(fmt.Sprintf("%s must be a duration between 100ms and 1h", key))
	}
	return d
}
