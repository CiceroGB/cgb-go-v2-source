FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY ./ /app
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o api .
RUN apk add --no-cache file
RUN file api

FROM alpine:latest
COPY --from=builder /app/api /usr/local/bin/
RUN chmod +x /usr/local/bin/api
CMD ["api"]
