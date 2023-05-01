#!/bin/bash

docker tag lambda-collector:latest ${AWS_USER}.dkr.ecr.${AWS_REGION}.amazonaws.com/aws-de-lambda-collector:latest
docker push ${AWS_USER}.dkr.ecr.${AWS_REGION}.amazonaws.com/aws-de-lambda-collector:latest
