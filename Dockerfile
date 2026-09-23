FROM golang:alpine

WORKDIR /app

# Install dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN go build -o /bin/dwaarpal cmd/server/main.go

# Expose HTTP and gRPC ports
EXPOSE 8080 50051

# Run the binary
CMD ["/bin/dwaarpal"]
