# Stage 1: Build
FROM golang:1.24-alpine AS builder

WORKDIR /app
RUN apk add --no-cache build-base sqlite-dev


# Copy the rest of your code
COPY . .

# # Cache dependencies
# COPY go.mod ./
# RUN go mod download
RUN go mod tidy

# Enable CGO
ENV CGO_ENABLED=1

# Build the Go binary
RUN go build -o app ./cmd/playground

# Stage 2: Minimal runtime image
FROM alpine:latest

ENV TZ=Asia/Jakarta
RUN apk add --no-cache tzdata && cp /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Install certificates (for HTTPS support)
RUN apk --no-cache add ca-certificates

WORKDIR /root/

# Copy binary from builder
COPY --from=builder /app/app .

# EXPOSE 8080
# Set executable entrypoint
CMD ["./app"]
