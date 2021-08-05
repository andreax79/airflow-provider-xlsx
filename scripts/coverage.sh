#!/usr/bin/env bash
set -e

export CURRENT_DIR=`realpath $(dirname "$0")`

source "${CURRENT_DIR}/activate.sh"

cd `dirname "$CURRENT_DIR"`
python3 -m coverage run --source=xlsx_provider setup.py test && python3 -m coverage report -m
