#!/usr/bin/env bash
echo
echo "-------------------------------------------------------------"
echo " Running QReceive: $(date)"
echo "-------------------------------------------------------------"

cd /app && uv run qreceive.py
