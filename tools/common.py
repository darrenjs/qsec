import datetime as dt
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import logging
import os
import json
from pathlib import Path


def save_dateframe(
    symbol: str,
    date: dt.date,
    df: pd.core.frame.DataFrame,
    sid: str,
    venue: str,
    dtype: str,
    interval: str
):
    date_str = date.strftime("%Y%m%d")
    home = str(Path.home())
    path = f"{home}/MDHOME/tickdata-parq/{dtype}/{venue}/{sid}/{date_str}"
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)
    fn = f"{path}/{sid}-{interval}-{date_str}.parq"

    # meta
    custom_meta = {"venue": venue, "symbol": symbol, "sid": sid, "dtype": dtype, "interval": interval}
    custom_meta_key = "qsec"
    table = pa.Table.from_pandas(df)
    custom_meta_json = json.dumps(custom_meta)
    existing_meta = table.schema.metadata
    combined_meta = {
        custom_meta_key.encode(): custom_meta_json.encode(),
        **existing_meta,
    }
    table = table.replace_schema_metadata(combined_meta)
    logging.info("writing parquet file '{}'".format(fn))
    pq.write_table(table, fn, compression="GZIP")


def short_contract_date(date: str) -> str:
    if len(date) != 6:
        raise Exception(f"expected date to have len 6, '{date[1]}'")
    year = date[1]
    mnth = int(date[2:4])
    mnthcode = ["F", "G", "H", "K", "M", "N", "Q", "U", "V", "X", "Z"][mnth - 1]
    return f"{mnthcode}{year}"


def build_assetid(symbol, shortExch="BNC", is_cash=False):

    if is_cash:
        return "_".join([symbol, shortExch])

    parts = symbol.split("_")
    if len(parts) == 1:
        return "_".join([symbol, "PF", shortExch])
    if len(parts) == 2:
        base, date = parts
        if date == "PERP":
            return "_".join([base, "PF", shortExch])
        else:
            return "_".join([base, short_contract_date(date), shortExch])
    else:
        raise Exception(f"invalid format for symbol, '{symbol}'")
