# qsec
Quant &amp; Systematic Crypto Research Tools

**-->WORK IN PROGRESS<--**

This repo is a collection of research tools to help in exploring and building systematic and quantitative crypto trading strategies.

**Using the Binance historic data download tools**

The `tools` folder has a script  `binance-fetch-trades.py` that can be
used for downloading historic trade data from Binance (Spot) exchange.
Obtaining and storing this data is an initial step to having a research
framework.

To run the script we provide the name of the symbol, and the date range.
For example, if we want the historic trades for coin-pair TVKUSDT for the date 13 Jan 2022, we would run this command:

```
python tools/binance-fetch-trades.py  --sym TVKUSDT --from 20220113 --upto 20220114
```


**CAUTION!**  downloading trades can take a very long time, so only download them if your research/backtest really needs them, and then download only for your required dates.  It's preferable to use/download kline/bar data, which are much faster to download.

_qsec_ is strongly opinionated on data storage. Data files are automatically stored under your home directory, under folder named MDHOME, in parquet files.
