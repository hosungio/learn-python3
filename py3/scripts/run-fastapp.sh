#!/bin/bash

FASTAPP_PATH=$1

FASTAPP=$(echo $FASTAPP_PATH | tr / .)
FASTAPP=${FASTAPP:0:(-3)}:app

echo
echo "Run fastapi app:" $FASTAPP
echo
uvicorn $FASTAPP \
  --host 0.0.0.0 \
  --port 9090 \
  --reload \
  --reload-dir codes
