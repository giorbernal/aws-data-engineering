#!/bin/bash

aws lambda create-function --region ${AWS_REGION} --function-name de-collection \
    --package-type Image  \
    --code ImageUri=${AWS_USER}.dkr.ecr.${AWS_REGION}.amazonaws.com/aws-de-lambda-collector:latest \
    --role arn:aws:iam::${AWS_USER}:role/de-lambda-collector \
    --timeout 600
