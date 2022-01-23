#!/usr/bin/env bash

set -e

script_dir=$(dirname "$(realpath -s "$0")")

qsec_home=$(dirname "$script_dir")

rm -v -f \
   binance_exchange-info.json \
   binance_usdfut_exchange-info.json \
   binance_coinfut_exchange-info.json \
   binance_assets.csv

export PYTHONPATH="${qsec_home}"

python ${qsec_home}/qsec/data/binance/fetch_binance_refdata.py
python ${qsec_home}/qsec/data/binance/parse_binance_refdata.py

# install into MDHOME

path=~/MDHOME/ref/assets/$(date +%Y%m%d)/assets-$(date +%Y%m%d).csv
lnk=~/MDHOME/ref/assets/latest

mkdir -p $(dirname $path)
cp -v binance_assets.csv $path
cd ~/MDHOME/ref/assets && ln -vsnf $path assets-latest.csv
