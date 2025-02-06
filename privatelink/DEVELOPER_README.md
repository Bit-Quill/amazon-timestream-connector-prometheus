# Timestream Prometheus Connector with AWS PrivateLink

## Overview

This guide explains how to set up Prometheus and the Prometheus Connector to ingest data to Amazon Timestream from within an isolated VPC environment using [AWS PrivateLink](https://aws.amazon.com/privatelink/).

This [serverless application](https://aws.amazon.com/serverless/) consists of the following:
- [Amazon EC2](https://aws.amazon.com/ec2/getting-started/) instance that will host Prometheus and the Prometheus Connector.
- [VPC Endpoints](https://docs.aws.amazon.com/whitepapers/latest/aws-privatelink/what-are-vpc-endpoints.html) for securely communicating with AWS services using PrivateLink.
- [Amazon ECR](https://aws.amazon.com/ecr/getting-started/) to store docker images that will be deployed in the EC2 instance.

This application assumes that the VPC in which the template will be deployed has no internet access and ensures that all communication stays within Amazon's internal network. 

## Prerequisites

1. An existing VPC with at least two private subnets and route tables.
1. An existing Timestream database and table.
2. Read and write cells for your Timestream account. Amazon routes requests to the write and query endpoints of the cell that your account has been mapped to for a given region. 

To get your assigned cells using `awscli`:

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

The following SAM template deploys an EC2 instance along with required VPC endpoints and resources for launching Prometheus and the Prometheus connector.

From your existing VPC, you will need the following values:
- VPC ID: This is the ID of your existing VPC
- VPC CIDR : This is the CIDR range for your VPC
- Private Subnet IDs: This is where the EC2 instance and VPC endpoints will be deployed
- Private Route Table IDs: This is how the [S3 Gateway endpoint](https://docs.aws.amazon.com/vpc/latest/privatelink/vpc-endpoints-s3.html) will resolve requests
- Query and Write cells: These are your assigned endpoint cells for Timestream


1. From the `privatelink` directory, run the following command to deploy the template:

```
sam deploy --parameter-overrides "VpcId=<VPC_ID> VpcCidrIp=<VPC_CIPR_IP> PrivateSubnetId1=<PRIVATE_SUBNET_ID_1> PrivateSubnetId2=<PRIVATE_SUBNET_ID_2> PrivateRouteTableId1=<PRIVATE_ROUTE_TABLE_ID_1> PrivateRouteTableId2=<PRIVATE_ROUTE_TABLE_ID_2> TimestreamQueryCell=<QUERY_CELL> TimestreamWriteCell=<WRITE_CELL>"
```

To view the full set of `sam deploy` options see the [sam deploy documentation](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/sam-cli-command-reference-sam-deploy.html).

2. The deployment will have the following outputs upon completion:

- `InstanceId`: ID of the EC2 instance
- `EcrRepositoryUrl`: URL of the ECR repository

   An example of the output:

```
------------------------------------------------------------------------------
Outputs                                                                                                                                           
------------------------------------------------------------------------------
Key                 InstanceId                                                                                                                    
Description         ID of the EC2 instance                                                                                                        
Value               i-08a5d7e1700c9be5a                                                                                                           

Key                 EcrRepositoryUrl                                                                                                              
Description         URL of the ECR repository                                                                                                     
Value               460629772345.dkr.ecr.us-west-2.amazonaws.com/privatelink                                                                      
------------------------------------------------------------------------------
```

### Prepare docker images

First, authenticate with docker on your local machine. This will allow you to push images to the ECR repository that was created from deployment. 


1. Replace `ECR_REPOSITORY_URL` with your ECR repository URL to authenticate with docker.

```
aws ecr get-login-password --region us-west-2 | docker login --username AWS --password-stdin <ECR_REPOSITORY_URL>
```

#### Prepare the Prometheus Connector

1. From this directory (`privatelink`), use the `arm.Dockerfile` to build and tag the Prometheus Connector.  Replace `ECR_REPOSITORY_URL` with your ECR repository URL.

```
docker build -t <ECR_REPOSITORY_URL>:prometheus-connector -f ./arm.Dockerfile ..
```

2. Push the built image to ECR.
```
docker push <ECR_REPOSITORY_URL>:prometheus-connector
```

#### Prepare Prometheus

Prometheus maintains an [official docker image](https://hub.docker.com/r/prom/prometheus) that can be used to directly tag and push to the ECR repository.

1. Tag the Prometheus image, replacing `ECR_REPOSITORY_URL` with your ECR repository URL.
```
docker tag prom/prometheus:latest <ECR_REPOSITORY_URL>:prometheus
```

2. Push the tagged image to ECR.
```
docker push <ECR_REPOSITORY_URL>:prometheus
```

You are now set to connect to the EC2 instance and deploy the containers.

### Connect to EC2

1. Start an AWS SSM session, replacing `INSTANCE_ID` with your EC2 instance ID from deployment.

```shell
aws ssm start-session --target i-<INSTANCE_ID>
``` 
**You should now be connected to your instance.**

2. Run the following command to configure your docker user and group:
```
sudo usermod -aG docker ssm-user
sudo newgrp docker
```

3. Login with docker to gain pull access to your ECR repository. This will prompt for you to enter your password.
```
docker login --username AWS <ECR_REPOSITORY_URL>
```

Use the same password that was retrieved for pushing the images to ECR. To retrieve the password, run the following command from your local environment (where your AWS profile is configured):

```
aws ecr get-login-password --region us-west-2
```

Once you have successfully logged in, you are now able to pull images from ECR through VPC endpoints.

### Launch Prometheus Connector


To deploy the Prometheus Connector from within the EC2, set the following environment variables to configure your existing Timestream database, region, and assigned cells:

- `DEFAULT_DATABASE`: Specifies the default Timestream database for the Prometheus connector.
- `DEFAULT_TABLE`: Specifies the default table for storing Prometheus metrics.
- `AWS_REGION`: Defines the AWS region.
- `QUERY_CELL`: Defines the query endpoint cell for Timestream.
- `INGEST_CELL`: Defines the ingestion endpoint cell for Timestream.


Launch the Prometheus Connector, replacing `ECR_REPOSITORY_URL` with your ECR repository URL:

```
docker run -d \
  --name connector \
  --network aws_network \
  -p 9201:9201 \
  -e AWS_ENABLE_ENDPOINT_DISCOVERY=false \
  <ECR_REPOSITORY_URL>:prometheus-connector \
  --default-database=${DEFAULT_DATABASE:-PrometheusDatabase} \
  --default-table=${DEFAULT_TABLE:-PrometheusMetricsTable} \
  --region=${AWS_REGION:-us-west-2} \
  --log.level=debug \
  --query-base-endpoint=https://${QUERY_CELL:-query-cell1}.timestream.${AWS_REGION:-us-west-2}.amazonaws.com \
  --write-base-endpoint=https://${INGEST_CELL:-ingest-cell1}.timestream.${AWS_REGION:-us-west-2}.amazonaws.com
```

Verify that the container is running by viewing its logs:

```
docker logs connector -f
```


### Launch Prometheus

To deploy Prometheus from within the EC2, you will need to create two files:
- `prom.yml`: This is the configuration file for Prometheus.
- `passwordFile`: A file that contains only the value for your account's *aws_secret_access_key*.

1. Create a directory for Prometheus and set up the following files.
```
mkdir ~/prometheus && touch ~/prometheus/{passwordFile,prom.yml}
```


- `~/prometheus/prom.yml`:
```yaml
scrape_configs:
  - job_name: 'prometheus'
    scrape_interval:    15s
    static_configs:
      - targets: ['localhost:9090']
remote_write:
  - url: "http://connector:9201/write"
    basic_auth:
      username: <ACCESS_KEY>
      password_file: /etc/prometheus/passwordFile
remote_read:
  - url: "http://connector:9201/read"
    basic_auth:
      username: <ACCESS_KEY>
      password_file: /etc/prometheus/passwordFile
```

Replace `ACCESS_KEY` with your AWS Access key.


- `~/prometheus/passwordFile`:
```yaml
<aws_secret_access_key>
```


You are now ready to deploy Prometheus.

2. Run the following docker command to launch Prometheus, replacing `ECR_REPOSITORY_URL` with your ECR repository URL:
```
docker run -d \
  --name prom \
  --network aws_network \
  -p 9090:9090 \
  -v ~/prometheus/passwordFile:/etc/prometheus/passwordFile \
  -v ~/prometheus/prom.yml:/etc/prometheus/prometheus.yml \
  <ECR_REPOSITORY_URL>:prometheus \
  --config.file=/etc/prometheus/prometheus.yml --log.level=debug
```

Verify that the container is running by viewing its logs.

```
docker logs prom -f
```
#### Verify ingestion

You can observe the logs from containers, or use `awscli` from your local machine to directly confirm that Prometheus data is being ingested to Timestream through the Prometheus Connector.

```shell
aws timestream-query query --query-string "SELECT count() FROM <PrometheusDatabase>.<PrometheusMetricsTable>" --region <AWS_REGION>
```

To view the Prometheus expression browser locally, you can connect to your EC2 with port-forwarding:
```
 aws ssm start-session --target i-<INSTANCE_ID> --document-name AWS-StartPortForwardingSession --parameters '{"portNumber":["9090"],"localPortNumber":["9090"]}'
```

Visit http://localhost:9090 in a browser. Execute a Prometheus Query Language (PromQL) query to verify ingestion.

A simple example:
```
prometheus_http_requests_total{}
```

For more details on verification, see [README.md#verification](../README.md#verification).


### Cleanup

1. Delete the cloudformation stack. From the `privatelink` directory, run the following command:

```shell
sam delete
```

## Caveats

This SAM template does not enable TLS encryption by default between Prometheus and the Prometheus Connector.

Ensure the following:

1. Regularly rotate IAM user access keys, see [rotating access keys](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html#Using_RotateAccessKey).
2. Follow IAM [best practices](https://docs.aws.amazon.com/timestream/latest/developerguide/security_iam_id-based-policy-examples.html#security_iam_service-with-iam-policy-best-practices).

## License

This project is licensed under the Apache 2.0 License.
