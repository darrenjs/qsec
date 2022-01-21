import datetime as dt
import requests
import json
import time
import pandas as pd
import logging
import pyarrow as pa
import pyarrow.parquet as pq
import os, sys
import numpy as np
import argparse


import qsec.logging
import qsec.time
import qsec.app
import common


api = "https://api.binance.com"


def call_http_fetch_klines(
    symbol, startTime: int, endTime: int, interval: str = "1m", limit: int = 1000
):
    path = "/api/v3/klines"
    options = {
        "symbol": symbol,
        "limit": 1000,
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


def call_http_trade(symbol, start_time=None, end_time=None, fromId=None):
    path = "/api/v3/aggTrades"
    options = {"symbol": symbol, "limit": 1000}
    if start_time is not None:
        options["startTime"] = start_time
    if end_time is not None:
        options["endTime"] = end_time
    if fromId is not None:
        options["fromId"] = fromId

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
        "ignore",
    ]
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
    requestLimit = 1000  # binance constraint

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
    for d in dates:
        df = fetch_klines_for_date(symbol, d, interval)
        common.save_dateframe(
            symbol, d, df, sid, "binance", f"bars-{interval}", interval
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
    parser.add_argument(
        "--interval",
        dest="interval",
        type=str,
        help="interval time",
        required=False,
        default="1m",
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
    valid_intervals = [
        "1m",
        "3m",
        "5m",
        "15m",
        "30m",
        "1h",
        "2h",
        "4h",
        "6h",
        "8h",
        "12h",
        "1d",
        "3d",
        "1w",
        "1M",
    ]

    # If specified interval by the user is not present in valid intervals, break
    if args.interval is not None:
        if args.interval not in valid_intervals:
            print(
                "Specified interval was not considered valid. Please set interval to one of the following:"
            )
            print("1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1M")
            sys.exit(1)

    interval = "1m" if args.interval is None else args.interval
    sid = common.build_assetid(args.sym, "BNC", is_cash=True)
    fetch(args.sym, fromDt, uptoDt, sid, interval)


if __name__ == "__main__":
    qsec.app.main(main)
