FROM golang:1.25-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/controlplane ./cmd/controlplane && CGO_ENABLED=0 go build -trimpath -o /out/worker ./cmd/worker && CGO_ENABLED=0 go build -trimpath -o /out/replay-result ./tests/recovery/replay
FROM docker:28-cli
COPY --from=build /out/ /usr/local/bin/
CMD ["controlplane"]
