#!/bin/bash

aws ecr batch-delete-image --repository-name aws-de-lambda-collector --image-ids imageTag=latest
