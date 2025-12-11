#!/bin/bash
set -e

sbt compile assembly
docker build -t meszka/sms-phishing-filter .
docker compose up -d --force-recreate
