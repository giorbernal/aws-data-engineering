#/bin/bash

poetry export --without-hashes --format=requirements.txt > requirements.txt
docker build -t lambda-collector .
rm requirements.txt
