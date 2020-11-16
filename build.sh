#!/usr/bin/env bash

git_hash=$(git rev-parse --short HEAD)

docker build . \
  -t "864879987165.dkr.ecr.us-east-1.amazonaws.com/calm/pgdatadiff:${git_hash}"
