#!/bin/bash

# Function to link policies to roles
function put_role_policy {
  POL_VER=$(aws iam get-policy --policy-arn "arn:aws:iam::$3:policy/$1" --query 'Policy.DefaultVersionId')
  aws iam get-policy-version --policy-arn "arn:aws:iam::$3:policy/$1" --version-id ${POL_VER:1:2} --query "PolicyVersion.Document" > temp.json
  aws iam put-role-policy --role-name de-lambda-collector --policy-name $2 --policy-document file://temp.json
  rm -f temp.json
}

# ECR Policy (It could not be necessary ??)
aws iam create-policy \
    --policy-name "AWSEcrBasic" \
    --description "Basic Permissions for ECR access for a lambda" \
    --policy-document "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Sid\":\"VisualEditor0\",\"Effect\":\"Allow\",\"Action\":[\"ecr:SetRepositoryPolicy\",\"ecr:GetRepositoryPolicy\"],\"Resource\":\"arn:aws:ecr:${AWS_REGION}:${AWS_USER}:repository/*\"}]}"

# Let's create our two functions:

# 1) Function to collect data in a scheduled way

COLLECTOR_FUNCTION_NAME=de-collector

# Managing role and permissions for lambda
aws iam create-policy --policy-name "AmazonDynamoDBWdpBatchWriteItem" \
    --description "Permission to write just BathWriteItem to my table weather-datapoints" \
    --policy-document "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Sid\":\"VisualEditor0\",\"Effect\":\"Allow\",\"Action\":\"dynamodb:BatchWriteItem\",\"Resource\":\"arn:aws:dynamodb:${AWS_REGION}:${AWS_USER}:table/weather-datapoints\"}]}"
aws iam create-role \
    --role-name de-lambda-collector \
    --description "Allows Lambda functions to call AWS services on your behalf." \
    --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
put_role_policy "AWSEcrBasic" "AWSEcrBasic" "${AWS_USER}"
put_role_policy "AmazonDynamoDBWdpBatchWriteItem" "AmazonDynamoDBWdpBatchWriteItem" "${AWS_USER}"
put_role_policy "AmazonS3FullAccess" "AmazonS3FullAccess" "aws"
put_role_policy "service-role/AWSLambdaBasicExecutionRole" "AWSLambdaBasicExecutionRole" "aws"


# Creating the lambda to collect data
aws lambda create-function --region ${AWS_REGION} --function-name ${COLLECTOR_FUNCTION_NAME} \
    --package-type Image  \
    --code ImageUri=${AWS_USER}.dkr.ecr.${AWS_REGION}.amazonaws.com/aws-de-lambda-collector:latest \
    --role arn:aws:iam::${AWS_USER}:role/de-lambda-collector \
    --timeout 600

# Creating the EventBridge rule to schedule the lambda
aws events put-rule --name "de-collector-schedule" --schedule-expression "cron(50 21 * * ? *)"

# Attach a the lambda as a target of the event
aws events put-targets --rule "de-collector-schedule" --targets "Id"="1","Arn"="arn:aws:lambda:${AWS_REGION}:${AWS_USER}:function:de-collector"

# Set resource permissions to the lambda function to allow the events access to the lambda
aws lambda add-permission \
    --function-name "${COLLECTOR_FUNCTION_NAME}" \
    --statement-id "AWSEvents_de-collector-schedule" \
    --action "lambda:InvokeFunction" \
    --principal "events.amazonaws.com" \
    --source-arn "arn:aws:events:${AWS_REGION}:${AWS_USER}:rule/de-collector-schedule"


# 2) Function to access data by an API Gateway
# TODO
