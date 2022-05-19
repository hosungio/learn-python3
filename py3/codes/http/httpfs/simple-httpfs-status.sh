#!/bin/bash

SIMPLE_HTTPFS=http.server

OUT=$(ps aux | grep $SIMPLE_HTTPFS | grep -v grep) 
if [ -z "$OUT" ]; then
  echo "simple-httpfs not started"
else
  echo "simple-httpfs is running:"
  OUT_ARR=($OUT)
  echo "  PID: ${OUT_ARR[1]}"
  echo "  PYTHON: ${OUT_ARR[10]}"
  echo "  APP_NAME: ${OUT_ARR[12]}"
  echo "  APP_ARGS: ${OUT_ARR[@]:13}"
fi
