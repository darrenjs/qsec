import datetime as dt
import requests
import json
import time
import pandas as pd
import logging
import pyarrow as pa
import pyarrow.parquet as pq
import os
import numpy as np
import argparse


import qsec.logging
import qsec.time
import qsec.app
import common


api="https://api.binance.com"


def call_http_fetch_klines(symbol,
                           startTime:int,
                           endTime:int,
                           interval:str = "1m",
                           limit:int = 1000):
    path="/api/v3/klines"
    options = {
        "symbol" : symbol,
        "limit" : 1000,
        "interval" : interval,
        "startTime" : startTime,
        "endTime" : endTime
    }
    url=f"{api}{path}"
    logging.info("making URL request: {}, options: {}".format(url, options))
    reply = requests.get(url, params=options)
    if reply.status_code != 200:
        raise Exception("http request failed, error-code {}, msg: {}".format(reply.status_code, reply.text))
    return reply.text


def call_http_trade(symbol, start_time = None, end_time = None, fromId=None):
    path="/api/v3/aggTrades"
    options = {
        "symbol" : symbol,
        "limit" : 1000
    }
    if start_time is not None:
        options['startTime'] = start_time
    if end_time is not None:
        options['endTime'] = end_time
    if fromId is not None:
        options['fromId'] = fromId

    url=f"{api}{path}"
    logging.info("making URL request: {}, options: {}".format(url, options))
    reply = requests.get(url, params=options)

    if reply.status_code != 200:
        raise Exception("http request failed, error-code {}, msg: {}".format(reply.status_code, reply.text))

    return reply.text


def normalise_klines(df):
    df = df.copy()
    columns = [
        "openTime",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "closeTime",
        "quoteAssetVolume",
        "numberOfTrades",
        "takerBuyBaseAssetVolume",
        "takerBuyQuoteAssetVolume",
        "ignore"]
    df.columns = columns
    df['openTime'] = pd.to_datetime(df['openTime'], unit='ms')
    df['closeTime'] = pd.to_datetime(df['closeTime'], unit='ms')
    for col in ["open", "high","low","close","volume",
                "quoteAssetVolume", "takerBuyBaseAssetVolume", "takerBuyQuoteAssetVolume"]:
        df[col] = df[col].astype('float')
    df['time'] = df['closeTime']
    df.set_index('time', inplace=True, verify_integrity=True)
    df.sort_index(inplace=True)
    df.drop(['ignore'], axis=1, inplace=True)
    return df


def fetch_klines_for_date(symbol:str, kline_date: dt.date):
    logging.info("fetching klines for date {}".format(kline_date))
    t0 = qsec.time.date_to_datetime(kline_date)
    t1 = qsec.time.date_to_datetime(kline_date + dt.timedelta(days=1))
    minutes = int((t1 - t0).total_seconds() / 60)  # TODO: detect any fraction?

    all_dfs = []
    requestLimit = 1000
    requests = [x for x in range(0, minutes, requestLimit)]
    for x in requests:
        begTime = t0 + dt.timedelta(minutes=x)
        endTime = begTime + dt.timedelta(minutes=requestLimit)
        logging.info("request bars for {} to {}".format(begTime, endTime))
        begTime = int(begTime.timestamp()*1000)
        endTime = int(endTime.timestamp()*1000)
        raw_json = call_http_fetch_klines(symbol, begTime, endTime)
        df = pd.DataFrame(json.loads(raw_json))
        logging.debug("returned {} rows".format(df.shape[0]))
        all_dfs.append(df)
    df = pd.concat(all_dfs)
    del all_dfs
    df = normalise_klines(df)

    # retain only rows within user requested period
    t0_ms = np.datetime64(int(t0.timestamp() * 1000), 'ms')
    t1_ms = np.datetime64(int(t1.timestamp() * 1000), 'ms')
    df = df.loc[(df.index >= t0_ms) & (df.index < t1_ms)]
    return df


def fetch(symbol:str, fromDt:dt.date, endDt:dt.date, sid:str):
    dates = qsec.time.dates_in_range(fromDt, endDt)
    for d in dates:
        df = fetch_klines_for_date(symbol, d)
        common.save_dateframe(symbol, d, df, sid, "binance", "bars")


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sym", type=str, help="symbol", required=True)
    parser.add_argument("--from", dest='fromDt', type=str, help="begin date", required=True)
    parser.add_argument("--upto", dest='uptoDt', type=str, help="to date", required=True)
    return parser.parse_args()


def process_args(args):
   try:
        fromDt = qsec.time.to_date(args.fromDt)
        uptoDt = qsec.time.to_date(args.uptoDt)
        if (fromDt >= uptoDt):
            raise qsec.app.EasyError("'from' date must be before 'upto' date")
        return fromDt, uptoDt
   except qsec.app.EasyError as e:
       raise qsec.app.EasyError(f"{e}")


def main():
    qsec.logging.init_logging()
    args = parse_args()
    fromDt, uptoDt = process_args(args)
    sid = common.build_assetid(args.sym, "BNC", is_cash=True)
    fetch(args.sym, fromDt, uptoDt, sid)


if __name__ == "__main__":
    qsec.app.main(main)
