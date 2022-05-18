#!/bin/bash

PYTEST_FILE_PATH=$1
#echo $PYTEST_FILE_PATH

# python -m pytest -s tests/code/pytest/test_hello.py
python -m pytest -x -s $PYTEST_FILE_PATH
