#!/bin/bash
sbt compile assembly
docker compose build
docker compose up -d --force-recreate
