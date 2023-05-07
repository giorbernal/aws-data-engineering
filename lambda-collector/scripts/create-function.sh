#!/bin/bash

COLLECTOR_FUNCTION_NAME=de-collector

# Creating the lambda to collect data
aws lambda create-function --region ${AWS_REGION} --function-name ${COLLECTOR_FUNCTION_NAME} \
    --package-type Image  \
    --code ImageUri=${AWS_USER}.dkr.ecr.${AWS_REGION}.amazonaws.com/aws-de-lambda-collector:latest \
    --role arn:aws:iam::${AWS_USER}:role/de-lambda-collector \
    --timeout 600

# Creating the EventBridge rule to schedule the lambda
aws events put-rule --name "de-collector-schedule" --schedule-expression "cron(0 0 * * ? *)"

# Attach a the lambda as a target of the event
aws events put-targets --rule "de-collector-schedule" --targets "Id"="1","Arn"="arn:aws:lambda:${AWS_REGION}:${AWS_USER}:function:de-collector"

# Set resource permissions to the lambda function to allow the events access to the lambda
aws lambda add-permission \
    --function-name "${COLLECTOR_FUNCTION_NAME}" \
    --statement-id "AWSEvents_de-collector-schedule" \
    --action "lambda:InvokeFunction" \
    --principal "events.amazonaws.com" \
    --source-arn "arn:aws:events:${AWS_REGION}:${AWS_USER}:rule/de-collector-schedule"
