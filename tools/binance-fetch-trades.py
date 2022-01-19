import datetime as dt
import requests
import json
import time
import pandas as pd
import logging
import pyarrow as pa
import pyarrow.parquet as pq
import argparse
import os

import qsec.logging
import qsec.time
import qsec.app
import common

api="https://api.binance.com"


def buyer_maker_to_aggr_side(buyer_is_maker: bool):
    if (buyer_is_maker):
        return -1
    else:
        return +1



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


def get_trades(symbol, dt_from, dt_to):
    ts_from = qsec.time.datetime_to_epoch_ms(dt_from)
    ts_to = qsec.time.datetime_to_epoch_ms(dt_to)

    logging.info(f"from: {ts_from}");
    logging.info(f"to: {ts_to}");

    # why 1 hour?
    one_hour = 60 * 60 * 1000
    window_size_max = one_hour

    all_trades = []

    # grab trades from current to current+next
    # if this fails, we decrease the window size

    ts0 = ts_from
    window_size_ms = one_hour
    while (ts0 < ts_to):

        while True:
            ts1 = ts0 + window_size_ms
            ts1 = min(ts1, ts_to)
            raw_json = call_http_trade(symbol, ts0, ts1)
            trades = json.loads(raw_json)
            if len(trades) >= 1000:
                window_size_ms = int(window_size_ms/2)
                print("halfing window, to {}".format(window_size_ms))
            else:
                break

        print("[{} -> {}] trades {}  ({:.1f} per sec)".format(
            qsec.time.epoch_ms_to_str(ts0),
            qsec.time.epoch_ms_to_str(ts1),
            len(trades),
            len(trades)*1000/window_size_ms ))
        ts0 = ts0 + window_size_ms


        window_size_ms = min(int(window_size_ms * 1.25), one_hour)
        # window_size_ms = int(window_size_ms * 1.25)
        # if window_size_ms > one_hour:
        #     window_size_ms = one_hour


        all_trades.extend(trades);
        time.sleep(0.1)
    return all_trades



def normalise(df):
    df = df.copy()
    renames = dict({"a":"tradeId",
                    "p":"price",
                    "q":"qty",
                    "f":"firstTradeId",
                    "l":"lastTradeId",
                    "T":"timestamp"})
    df.rename(columns=renames, inplace=True)

    df['time'] = pd.to_datetime(df['timestamp'], unit='ms')
    df.drop('timestamp', axis=1, inplace=True)
    df["side"] = df.apply(lambda r: buyer_maker_to_aggr_side(r["m"]), axis=1) if len(df) else pd.Series()
    df.drop(['m', 'firstTradeId', 'lastTradeId'], axis=1, inplace=True)

    df.set_index('time', inplace=True)
    df.sort_index(inplace=True)

    df['price'] = df['price'].astype('float')
    df['qty'] = df['qty'].astype('float')


    #df['symbol'] = symbol
    return df


def plus_one_hour(t_milliseconds: int):
    return t_milliseconds + (60*60)*1000


def find_any_trade_in_period(symbol: str, beg_ms, end_ms):
    logging.info("searching for any trade within window of interest")
    # return 1025455339
    # 1025422081 1025455339
    end_time = plus_one_hour(beg_ms)
    print("requesting range {} to {}".format(
        qsec.time.epoch_ms_to_dt(beg_ms),
        qsec.time.epoch_ms_to_dt(end_time)))
    raw_json = call_http_trade(symbol, start_time=beg_ms, end_time=end_time)
    trades = pd.DataFrame(json.loads(raw_json))
    if len(trades):
        return  min(trades['a'])
    else:
        return None


def find_earliest_trade(symbol, beg_ms, end_ms, seek_trade_id):
    logging.info("searching for earliest trade within window of interest")
    # return 1025422081
    earliest_trade_id = seek_trade_id
    while True:
        seek_trade_id = earliest_trade_id - 1000
        raw_json = call_http_trade(symbol, fromId=seek_trade_id)
        trades = pd.DataFrame(json.loads(raw_json))
        trades = trades.set_index('a').sort_index()
        trades = trades[trades['T'] >= beg_ms]
        trades = trades[trades['T'] <= end_ms]
        if len(trades.index) == 0:
            break
        min_trade_id = min(trades.index)
        if min_trade_id == earliest_trade_id:
            break
        print("found new earliest trade id {}".format(min_trade_id))
        earliest_trade_id = min_trade_id
    return earliest_trade_id


def fetch_all_trades(symbol:str, beg_ms:int, end_ms:int, from_id:int):
    logging.info("fetching all trades for window")
    all_dfs = []
    cursor = from_id
    count = 0
    while True:
        # fetch trades for current ID range
        raw_json = call_http_trade(symbol, fromId=cursor)
        trades = pd.DataFrame(json.loads(raw_json))
        trades = trades[trades['T'] >= beg_ms]
        trades = trades[trades['T'] <= end_ms]
        if len(trades) == 0:
            break
        all_dfs.append(trades)
        count += len(trades)
        cursor = max(trades['a']) + 1
        highest_time = max(trades['T'])
        logging.info("trades: {}, time: {}".format(count, qsec.time.epoch_ms_to_dt(highest_time)))

    df = pd.concat(all_dfs)
    del all_dfs
    df = normalise(df)
    return df



def list_missing_ids(df):
    if df.shape[0] == 0:
        return []

    if df.shape[0] == 1 + min(df['tradeId']) - max(df['tradeId']):
        return []

    missing = []
    expected = df['tradeId'][0]
    for x in df['tradeId']:
        while expected != x:
            missing.append(x)
            expected += 1
        expected += 1
    return missing



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


def fetch_trades_for_date(symbol:str, kline_date: dt.date):
    logging.info("fetching trades for date {}".format(kline_date))
    t0 = qsec.time.date_to_datetime(kline_date)
    t1 = qsec.time.date_to_datetime(kline_date + dt.timedelta(days=1))
    t0 = int(t0.timestamp()*1000)
    t1 = int(t1.timestamp()*1000)

    seek_trade_id = find_any_trade_in_period(symbol, t0, t1)
    print(f"initial seek tradeId: {seek_trade_id}")
    earliest_trade_id = find_earliest_trade(symbol, t0, t1, seek_trade_id)
    print(f"window earliest tradeId: {earliest_trade_id}")
    df = fetch_all_trades(symbol, t0, t1, earliest_trade_id)
    missingIds = list_missing_ids(df)
    if len(missingIds) == 0:
        logging.info("no missing tradeIds detected")
    return df


def fetch(symbol:str, fromDt:dt.date, endDt:dt.date,
          sid:str):
    dates = qsec.time.dates_in_range(fromDt, endDt)
    for d in dates:
        df = fetch_trades_for_date(symbol, d)
        common.save_dateframe(symbol, d, df, sid, "binance", "trades")


def main():
    qsec.logging.init_logging()
    args = parse_args()
    fromDt, uptoDt = process_args(args)
    sid = common.build_assetid(args.sym, "BNC", is_cash=True)
    fetch(args.sym, fromDt, uptoDt, sid)


if __name__ == "__main__":
    main()
