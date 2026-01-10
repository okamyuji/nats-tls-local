FROM golang:1.21-alpine AS builder

WORKDIR /app
COPY go.mod ./
COPY main.go ./
RUN go mod tidy && go mod download
RUN CGO_ENABLED=0 go build -o log-publisher main.go

FROM alpine:latest
RUN apk --no-cache add ca-certificates
COPY --from=builder /app/log-publisher /usr/local/bin/
CMD ["log-publisher"]
