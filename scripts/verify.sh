#!/bin/sh
# Keep shell quoting out of Windows PowerShell's native argument marshalling.
test -z "$(gofmt -l cmd internal db tests)" &&
    go vet -tags integration ./... &&
    go build ./... &&
    go test -race -tags integration -count=1 ./...
