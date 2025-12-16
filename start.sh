#!/bin/bash
set -e

sbt compile assembly
docker build -t meszka/philter .
docker compose up -d --force-recreate
