#!/bin/bash

PYMOD_PATH=$1

PYMOD=$(echo $PYMOD_PATH | tr / .)
PYMOD=${PYMOD:0:(-3)}

echo
echo "Run python module: " $PYMOD
echo
python -m $PYMOD
