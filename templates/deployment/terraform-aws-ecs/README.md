## AWS ECS (Fargate) Scheduled Deployment

This deployment example uses AWS ECS Fargate to deploy Grove into AWS. This configures
Grove to execute every 10-minutes, and allows configuration of connections using JSON
documents placed under the `connectors/` directory in this folder.

To deploy using this template, Terraform should be installed on the machine used to
deploy Grove.

1. Login to an AWS account with the required permissions to deploy new services on the command-line.
2. Use Terraform to create the infrastructure required. You will be prompted for the name of the S3 bucket to create to output collected logs to.
    1. `terraform init`
    1. `terraform plan`
    1. `terraform apply`
3. Note the output ECR repository URL, as this is required to publish a Grove container image to.

This deployment requires a container image to be created and pushed into the created ECR
repository. The steps for building this image using Docker can be found below:

1. Build a new image using the `Dockerfile` in the root of this repository.
    1. `docker image build -t grove:latest`

To authenticate with AWS ECR, tag and publish the container image ready for use, please
follow the AWS documentation on ["Publishing a Docker image"](https://docs.aws.amazon.com/AmazonECR/latest/userguide/docker-push-ecr-image.html).