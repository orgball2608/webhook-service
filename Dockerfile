FROM golang:1.24.0 as service-builder

# Set destination for COPY
WORKDIR /app

# Download Go modules
COPY . ./
COPY go.mod .
COPY go.sum .

# Build
RUN CGO_ENABLED=0 GOOS=linux go build -o webhook_service .

EXPOSE 8080
EXPOSE 9090
CMD /app/webhook_service
