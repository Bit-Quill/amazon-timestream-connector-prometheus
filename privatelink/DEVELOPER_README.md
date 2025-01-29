# Timestream Prometheus Connector with AWS PrivateLink

## Overview

This guide explains how to set up the Prometheus Connector to send data to Amazon Timestream from within an isolated VPC environment. The setup process involves:

1. Creating a VPC and deploying an EC2 instance within a private subnet
2. Temporarily enabling internet access to download necessary code and dependencies
3. Revoking internet access to create a fully isolated environment
4. Establishing connectivity between the Prometheus Connector and Amazon Timestream using AWS PrivateLink through VPC endpoints

This architecture demonstrates secure data ingestion from Prometheus to Timestream without requiring public internet access.

## Pre-requisites

1. An existing Timestream database and table.
2. Read and write cells for your Timestream account.

To get your assigned cell endpoint:

For read endpoint:
```
aws timestream-query describe-endpoints --region <AWS_REGION>
```

For write endpoint:
```
aws timestream-write describe-endpoints --region <AWS_REGION>
```

Example output for the write endpoint:
```
{
    "Endpoints": [
        {
            "Address": "ingest-cell1.timestream.us-west-2.amazonaws.com",
            "CachePeriodInMinutes": 1440
        }
    ]
}
```
Take note of your assigned cells (`ingest-cell1` for the above example) for both read and write endpoints.


## Deployment
### 1. Deploy VPC

Deploy the VPC using the `./vpc/full-vpc.yml` template.

1. `cd ./vpc`
2. `sam deploy -t full-vpc.yml --parameter-overrides "TimestreamQueryCell=<QUERY_CELL> TimestreamWriteCell=<WRITE_CELL>"`

Where `<QUERY_CELL>`, `<WRITE_CELL>` are your assigned cells from the pre-requisite steps.

Take note of the VPC and subnet IDs from the deployment output.

### 2. Deploy EC2

Deploy the EC2 instance. This is where we will setup Prometheus and the Prometheus Connector.

1. `cd ./ec2`
2. `sam deploy --parameter-overrides "VpcId=<VPC_ID> PrivateSubnetId1=<PRIVATE_SUBNET_ID_1> PrivateSubnetId2=<PRIVATE_SUBNET_ID_2>"`

Once the EC2 instance has been successfully deployed (you may have to wait a few minutes after deployment for the instance to finish booting up), connect using AWS SSM:

```shell
aws ssm start-session --target i-<INSTANCE_ID>
``` 

### 3. Configure environment

From within the EC2 instance, run the following command to configure docker & docker-compose:
```
sudo usermod -aG docker ssm-user
sudo newgrp docker
alias dc="docker-compose"
export PATH=$PATH:/usr/local/bin
```

#### Configure Prometheus

1. `mkdir ~/prom`
2. `touch ~/prom/{passwordFile,prom.yml,docker-compose.yaml}`

And fill in the above files with the following configs:

`docker-compose.yml`:
```yaml
services:
  prometheus:
    image: prom/prometheus
    container_name: prom
    command: --config.file=/etc/prometheus/prometheus.yml --log.level=debug
    ports:
      - "9090:9090"
    volumes:
      - ./passwordFile:/etc/prometheus/passwordFile
      - ./prom.yml:/etc/prometheus/prometheus.yml
    networks:
      - aws_network

networks:
  aws_network:
    external: true
```

`prom.yml`:
```yaml
   scrape_configs:
     - job_name: 'prometheus'
       scrape_interval:    15s
       static_configs:
         - targets: ['localhost:9090']

   remote_write:
   - url: "http://connector:9201/write"

     # Update the username and password to a valid IAM access key and secret access key.
     basic_auth:
         username: accessKey
         password_file: passwordFile

   remote_read:
   - url: "http://connector:9201/read"

     # Update the username and password to a valid IAM access key and secret access key.
     basic_auth:
         username: accessKey
         password_file: passwordFile
```

Replace `accessKey` with your AWS Access key.

`passwordFile`:
```
<aws_secret_access_key>
```

The password file must contain only the value for *aws_secret_access_key*.

3. Run the following command to pull the Prometheus image:
```shell
cd ~/prom && dc pull
```

#### Configure Prometheus Connector

You can build the Prometheus Connector from source or pull a pre-built docker image.

##### Building from source

1. Clone the repo and check out the `dev-privatelink` branch.
2. Update `./privatelink/docker-compose.yaml` with your Timestream database and table, region and assigned cell endpoints.
2. `cd ./privatelink` and run `dc build`.

##### Using pre-built docker image

1. `mkdir ~/connector`
2. `touch ~/connector/docker-compose.yaml`

`docker-compose.yaml`:
```yaml
services:
  timestream-prometheus-connector:
    container_name: connector
    image: fpimproving/amazon-timestream-connector-prometheus:privatelink
    ports:
      - "9201:9201"
    command:
      - --default-database=${DEFAULT_DATABASE:-DevPrometheusDatabase}
      - --default-table=${DEFAULT_TABLE:-DevPrometheusMetricsTable}
      - --region=${AWS_REGION:-us-west-2}
      - --log.level=debug
      - --read-base-endpoint=https://<QUERY_CELL>.timestream.${AWS_REGION:-us-west-2}.amazonaws.com
      - --write-base-endpoint=https://<WRITE_CELL>.timestream.${AWS_REGION:-us-west-2}.amazonaws.com
    environment:
      AWS_ENABLE_ENDPOINT_DISCOVERY: false
    networks:
      - aws_network

networks:
  aws_network:
    external: true
```
Where `<QUERY_CELL>`, `<WRITE_CELL>` are your assigned cells from the pre-requisite steps.

3. Run the following command to pull the Prometheus Connector image:
```shell
cd ~/connector && dc pull
```

Your EC2 instance is now fully configured and you can safely revoke internet access for your VPC.

### 4. Revoke internet access

The `./vpc/private-vpc.yml` template contains the same resources as `./vpc/full-vpc.yml`, but excludes resources associated with providing the VPC internet access.

Deploy the `./vpc/private-vpc.yml` template to update the current VPC:

1. `cd ./vpc`
2. `sam deploy -t private-vpc.yml --parameter-overrides "TimestreamQueryCell=<QUERY_CELL> TimestreamWriteCell=<WRITE_CELL>"`

### 5. Launch Prometheus & Prometheus Connector

You can now bring up Prometheus and the Prometheus Connector to verify ingestion from within the EC2 instance. The current environment ensures network calls stay within the isolated VPC and has access to Amazon Timestream through VPC endpoints.

#### Start Prometheus Connector

1. Navigate to connector directory

- From source: `cd <path/to/amazon-timestream-connector-prometheus>`
- Using pre-built docker image: `cd ~/connector`

2. `dc up -d`

#### Start Prometheus

1. `cd ~/prom`
2. `dc up -d`

#### Verify ingestion

You can observe the logs from containers, or use the following command to confirm that Prometheus data is being ingested to Timestream through the Prometheus Connector.

```shell
aws timestream-query query --query-string "SELECT count() FROM <PrometheusDatabase>.<PrometheusMetricsTable>" --region <AWS_REGION>
```
