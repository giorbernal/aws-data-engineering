#/bin/bash

IMAGE_ID=$(docker images | grep lambda-collector | awk '{print $3}')
docker rmi ${IMAGE_ID}
