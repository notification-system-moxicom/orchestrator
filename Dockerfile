FROM golang:1.25.4-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 go build -o /app/orchestrator ./cmd/api-gateway/main.go

FROM alpine:3.21

RUN apk add --no-cache ca-certificates

WORKDIR /app

COPY --from=builder /app/orchestrator .
COPY configs/ configs/

EXPOSE 8081

ENTRYPOINT ["./orchestrator"]
CMD ["-c", "configs/config.docker.yaml"]
