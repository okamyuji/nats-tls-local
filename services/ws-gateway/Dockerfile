FROM golang:1.21-alpine AS builder

WORKDIR /app
COPY go.mod ./
COPY main.go ./
RUN go mod tidy && go mod download
RUN CGO_ENABLED=0 go build -o ws-gateway main.go

FROM alpine:latest
RUN apk --no-cache add ca-certificates
COPY --from=builder /app/ws-gateway /usr/local/bin/
EXPOSE 8080
CMD ["ws-gateway"]
