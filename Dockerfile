FROM golang:1.21-alpine

WORKDIR /app

# Install dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN go build -o /bin/dwaarpal cmd/server/main.go

# Expose port
EXPOSE 8080

# Run the binary
CMD ["/bin/dwaarpal"]
