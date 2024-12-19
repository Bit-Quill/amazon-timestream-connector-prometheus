# Correctness Testing for Prometheus Connector

## Prerequisites
1. Ensure your docker version is >= `20.10.0`, or you have `docker-compose` installed
2. Update `docker-compose.yml` in this directory with your AWS credentials

## How to execute tests
1. Bring up containers with `docker compose up -d`
2. Run tests with `docker exec -it mockmetheus pytest`

### Notes

The stub (`prom_pb2.py`) is generated using:

`protoc --python_out=. prom.proto`

### Resources:

Referenced Prometheus protobuf definitions:
- https://github.com/prometheus/prometheus/blob/main/prompb/remote.proto
- https://github.com/prometheus/prometheus/blob/main/prompb/types.proto
