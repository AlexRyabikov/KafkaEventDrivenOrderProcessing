FROM golang:1.23-alpine AS builder

ARG SERVICE
WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY pkg ./pkg
COPY services ./services

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/service ./services/${SERVICE}

FROM alpine:3.20
WORKDIR /app
COPY --from=builder /out/service /usr/local/bin/service
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/service"]
