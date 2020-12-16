#!/bin/bash
source gc_env/bin/activate
python extract.py

git add .
git commit -m "Estrazione dati giornaliera"
git push