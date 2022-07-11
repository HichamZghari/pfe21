#!/bin/bash
for file in `ls ini_files`;
do
    python -B main_market_place.py .\\ini_files\\$file
done
