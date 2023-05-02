#!/bin/bash

aws lambda delete-function --region ${AWS_REGION} --function-name de-collection
