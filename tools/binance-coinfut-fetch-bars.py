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

api = "https://dapi.binance.com"

def call_http_fetch_klines(
    symbol, startTime: int, endTime: int, interval: str = "1m", limit: int = 1500
):
    path = "/dapi/v1/klines"
    options = {
        "symbol": symbol,
        "limit": limit,
        "interval": interval,
        "startTime": startTime,
        "endTime": endTime,
    }
    url = f"{api}{path}"
    logging.info("making URL request: {}, options: {}".format(url, options))
    reply = requests.get(url, params=options)
    if reply.status_code != 200:
        raise Exception(
            "http request failed, error-code {}, msg: {}".format(
                reply.status_code, reply.text
            )
        )
    return reply.text


def normalise_klines(df):
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
        "ignore",
    ]

    if df.empty:
        return pd.DataFrame(columns=columns)

    df = df.copy()
    df.columns = columns
    df["openTime"] = pd.to_datetime(df["openTime"], unit="ms")
    df["closeTime"] = pd.to_datetime(df["closeTime"], unit="ms")
    for col in [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "quoteAssetVolume",
        "takerBuyBaseAssetVolume",
        "takerBuyQuoteAssetVolume",
    ]:
        df[col] = df[col].astype("float")
    df["time"] = df["closeTime"]
    df.set_index("time", inplace=True, verify_integrity=True)
    df.sort_index(inplace=True)
    df.drop(["ignore"], axis=1, inplace=True)
    return df


def fetch_klines_for_date(symbol: str, kline_date: dt.date, interval: str):
    logging.info("fetching klines for date {}".format(kline_date))

    # t0 and t1 and the start and end times of the date range
    t0 = qsec.time.date_to_datetime(kline_date)
    t1 = qsec.time.date_to_datetime(kline_date + dt.timedelta(days=1))

    all_dfs = []
    requestLimit = 1500  # binance constraint

    expected_rows = None
    if interval == "1m":
        expected_rows = 1440

    lower = t0
    while lower < t1:
        # calc the upper range of the next request
        upper = min(t1, lower + dt.timedelta(minutes=requestLimit))

        # make the request
        req_lower = int(lower.timestamp() * 1000)
        req_upper = int(upper.timestamp() * 1000)
        raw_json = call_http_fetch_klines(symbol, req_lower, req_upper, interval)
        df = pd.DataFrame(json.loads(raw_json))
        reply_row_count = df.shape[0]
        logging.debug(f"request returned {reply_row_count} rows")

        # trim the returned dataframe to be within our request range, just in
        # case exchange has returned additional rows
        if not df.empty:
            df = df.loc[(df[0] >= req_lower) & (df[0] < req_upper)]
            if df.shape[0] != reply_row_count:
                logging.info(
                    "retained {} rows of {} within actual request range".format(
                        df.shape[0], reply_row_count
                    )
                )
        all_dfs.append(df)
        lower = upper
        del df, upper, req_lower, req_upper, raw_json, reply_row_count

    df = pd.concat(all_dfs)
    del all_dfs
    df = normalise_klines(df)
    if df.empty:
        logging.warning(f"no data retrieved for {symbol} @ {kline_date}")

    # retain only rows within user requested period
    t0_ms = np.datetime64(int(t0.timestamp() * 1000), "ms")
    t1_ms = np.datetime64(int(t1.timestamp() * 1000), "ms")
    df = df.loc[(df.index >= t0_ms) & (df.index < t1_ms)]

    if expected_rows and df.shape[0] != expected_rows:
        logging.warning(
            "row count mismatch; expected {}, actual {}".format(
                expected_rows, df.shape[0]
            )
        )
    return df


def fetch(symbol: str, fromDt: dt.date, endDt: dt.date, sid: str, interval: str):
    dates = qsec.time.dates_in_range(fromDt, endDt)
    venue = "binance_coinfut"
    for d in dates:
        fn = common.build_md_item_filename(sid, d, f"bars{interval}", venue, f"bars{interval}")
        if os.path.exists(fn):
            logging.info("data item exists, skipping: '{}'".format(fn))
            continue
        df = fetch_klines_for_date(symbol, d, interval)
        common.save_dateframe(
            symbol, d, df, sid, venue, f"bars{interval}", f"bars{interval}"
        )


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sym", type=str, help="symbol", required=True)
    parser.add_argument(
        "--from", dest="fromDt", type=str, help="begin date", required=True
    )
    parser.add_argument(
        "--upto", dest="uptoDt", type=str, help="to date", required=True
    )
    return parser.parse_args()


def process_args(args):
    try:
        fromDt = qsec.time.to_date(args.fromDt)
        uptoDt = qsec.time.to_date(args.uptoDt)
        if fromDt >= uptoDt:
            raise qsec.app.EasyError("'from' date must be before 'upto' date")
        return fromDt, uptoDt
    except qsec.app.EasyError as e:
        raise qsec.app.EasyError(f"{e}")


def main():
    qsec.logging.init_logging()
    args = parse_args()
    fromDt, uptoDt = process_args(args)
    sid = common.build_assetid(args.sym, "BNC")
    interval = "1m"
    fetch(args.sym, fromDt, uptoDt, sid, interval)


if __name__ == "__main__":
    qsec.app.main(main)
