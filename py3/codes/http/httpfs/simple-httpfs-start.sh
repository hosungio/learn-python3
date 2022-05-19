#!/bin/bash

BIND_PORT=12900
BIND_IF=127.0.0.1
SERVE_DIR=/tmp

python -m http.server $BIND_PORT --bind $BIND_IF --directory $SERVE_DIR &
