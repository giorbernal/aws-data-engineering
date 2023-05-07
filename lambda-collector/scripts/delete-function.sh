#!/bin/bash

COLLECTOR_FUNCTION_NAME=de-collector

# Let's remove the EventBridge target
aws events remove-targets --rule "de-collector-schedule" --ids "1"

# Let's remove the rule
aws events delete-rule --name "de-collector-schedule"

# Let's remove the function
aws lambda delete-function --region ${AWS_REGION} --function-name ${COLLECTOR_FUNCTION_NAME}
