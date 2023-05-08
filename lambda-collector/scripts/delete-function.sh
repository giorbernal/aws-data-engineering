#!/bin/bash

# Let's delete our two functions:

# 1) Function to collect data in a scheduled way

COLLECTOR_FUNCTION_NAME=de-collector

# Let's remove the EventBridge target
aws events remove-targets --rule "de-collector-schedule" --ids "1"

# Let's remove the rule
aws events delete-rule --name "de-collector-schedule"

# Let's remove the function
aws lambda delete-function --region ${AWS_REGION} --function-name ${COLLECTOR_FUNCTION_NAME}

# Let's delete role and ad-hoc policies
aws iam delete-role-policy --role-name de-lambda-collector --policy-name AmazonDynamoDBWdpBatchWriteItem
aws iam delete-role-policy --role-name de-lambda-collector --policy-name AWSEcrBasic
aws iam delete-role-policy --role-name de-lambda-collector --policy-name AmazonS3FullAccess
aws iam delete-role-policy --role-name de-lambda-collector --policy-name AWSLambdaBasicExecutionRole
aws iam delete-role --role-name de-lambda-collector
aws iam delete-policy --policy-arn "arn:aws:iam::${AWS_USER}:policy/AmazonDynamoDBWdpBatchWriteItem"


# 2) Function to access data by an API Gateway
# TODO

# Let's delete general policy for
aws iam delete-policy --policy-arn "arn:aws:iam::${AWS_USER}:policy/AWSEcrBasic"
