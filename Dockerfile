FROM golang:1.24-alpine AS builder

# Set working directory
WORKDIR /app

# Copy go.mod and go.sum files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy the source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -o /go-kms

# Create a minimal runtime image
FROM alpine:latest
RUN apk --no-cache add ca-certificates

WORKDIR /

# Copy the binary from the builder stage
COPY --from=builder /go-kms /go-kms

# Expose any necessary ports
# EXPOSE 8080  # Uncomment if your application serves HTTP

# Set the entry point
ENTRYPOINT ["/go-kms"]