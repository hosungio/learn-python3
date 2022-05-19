#!/bin/bash

SIMPLE_HTTPFS=http.server

PID=$(ps aux | grep $SIMPLE_HTTPFS | grep -v grep | awk {'print $2'})
if [ -z "$PID" ]; then
  echo "simple-httpfs not started"
else
  echo "stop simple-httpfs: $PID"
  kill $PID
fi
