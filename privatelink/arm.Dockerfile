FROM arm64v8/amazonlinux:latest AS build_stage
RUN yum update -y
RUN yum install -y gzip tar

# Install Go
ADD https://go.dev/dl/go1.22.3.linux-arm64.tar.gz .
RUN tar -xvf go1.22.3.linux-arm64.tar.gz
RUN mv go /usr/local

ENV PATH="${PATH}:/usr/local/go/bin"
ENV GOPATH="/go"
ENV GOBIN="/usr/local/go/bin"

# Set Docker image metadata
LABEL name="timestream/timestream-prometheus-connector" \
      summary="Amazon Timestream Prometheus Connector" \
      description="This Prometheus connector receives and sends samples between Prometheus and Timestream through Prometheus' remote write and remote read protocols."

WORKDIR /tmp/timestream/

# Copy go.mod and go.sum and download all required dependencies
COPY go.mod .
COPY go.sum .
RUN go mod download

COPY . .

# Set environment variables for ARM64
ENV GOOS=linux
ENV GOARCH=arm64
ENV CGO_ENABLED=0

# Build the binary for ARM64.
RUN go build -o ./timestream-prometheus-connector .

# Stage 2: Copy the pre-compiled Linux binary to the final image
FROM arm64v8/amazonlinux:latest AS copy_stage
RUN yum install -y ca-certificates

COPY --from=build_stage /tmp/timestream/timestream-prometheus-connector /app/timestream/timestream-prometheus-connector

# Expose service endpoint
EXPOSE 9201

# Run the container
ENTRYPOINT ["./app/timestream/timestream-prometheus-connector"]
