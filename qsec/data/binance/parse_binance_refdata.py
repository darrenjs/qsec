import json
import logging
import sys
import pandas as pd

import qsec.logging


def parse_binance_spot_exchange_info(fn):
    venue = "binance"
    assetType = "coinpair"
    logging.info("reading file '{}'".format(fn))
    # read json
    with open(fn) as f:
        data = json.load(f)
    symbols = data["symbols"]
    logging.info("file has {} symbols".format(len(symbols)))
    rows = []

    filters_to_ignore = set(
        ["MAX_NUM_ALGO_ORDERS", "ICEBERG_PARTS", "MARKET_LOT_SIZE", "PERCENT_PRICE"]
    )

    logging.info("ignoring following filters: {}".format(filters_to_ignore))
    for item in symbols:
        #     import ipdb; ipdb.set_trace();
        row = dict()
        row["symbol"] = item["symbol"]
        row["assetid"] = item["symbol"] + "_BNC"
        row["type"] = assetType
        row["venue"] = venue
        row["baseAsset"] = item["baseAsset"]
        row["quoteAsset"] = item["quoteAsset"]
        row["quoteAsset"] = item["quoteAsset"]
        row["quoteAssetPrecision"] = item["quoteAssetPrecision"]
        row["baseAssetPrecision"] = item["baseAssetPrecision"]
        row["status"] = item["status"]

        # TODO: here I am not supporting the MARKET_LOT_SIZE filter
        for filter in item["filters"]:
            filterType = filter["filterType"]
            if filterType == "LOT_SIZE":
                row["minQty"] = filter["minQty"]
                row["maxQty"] = filter["maxQty"]
                row["lotQty"] = filter["stepSize"]
            elif filterType == "MIN_NOTIONAL":
                row["minNotional"] = filter["minNotional"]
            elif filterType == "PRICE_FILTER":
                row["tickSize"] = filter["tickSize"]
            elif filterType == "MAX_NUM_ORDERS":
                row["maxNumOrders"] = filter["maxNumOrders"]
            elif filterType in filters_to_ignore:
                pass
            else:
                logging.warn("ignoring binance filter '{}'".format(filterType))
        rows.append(row)

    df = pd.DataFrame(rows)
    return df


def normalise_contractType(contractType: str) -> str:
    if contractType == "PERPETUAL":
        return "perp", "PF"
    if contractType in ["CURRENT_QUARTER", "NEXT_QUARTER"]:
        return "future", ""
    print(f"unhandled contract type: {contractType}")
    return None, None


def simplify_future_native_code(symbol):
    parts = symbol.split("_")
    if len(parts) != 2:
        raise Exception(f"expected symbol to split into 2 parts, '{symbol}'")
    if len(parts[1]) != 6:
        raise Exception(f"expected symbol-date to have len 6, '{parts[1]}'")

    year = parts[1][0:2]
    mnth = int(parts[1][2:4])
    mnthcode = ["F", "G", "H", "K", "M", "N", "Q", "U", "V", "X", "Z"][mnth - 1]
    return "_".join([parts[0], mnthcode + year[1], "BNC"])


def parse_binance_usdfut_exchange_info(fn, venue):
    logging.info("reading file '{}'".format(fn))
    # read json
    with open(fn) as f:
        data = json.load(f)
    symbols = data["symbols"]
    logging.info("file has {} symbols".format(len(symbols)))
    rows = []

    filters_to_ignore = set(
        [
            "MAX_NUM_ALGO_ORDERS",
            "ICEBERG_PARTS",
            "MARKET_LOT_SIZE",
            "PERCENT_PRICE",  # <=== could be useful to enable
        ]
    )

    logging.info("ignoring following filters: {}".format(filters_to_ignore))
    for item in symbols:
        row = dict()
        symbol = item["symbol"]
        row["symbol"] = symbol

        assetType, assetShortType = normalise_contractType(item["contractType"])
        if assetType is None:
            logging.info(f"skipping asset '{symbol}'")
            continue

        row["assetid"] = symbol
        if assetType == "perp" and not symbol.endswith("_PERP"):
            row["assetid"] = f"{symbol}_PF_BNC"
        elif assetType == "perp" and symbol.endswith("_PERP"):
            row["assetid"] = symbol.replace("_PERP", "_PF") + "_BNC"
        else:
            row["assetid"] = f"{symbol}_BNC"

        if assetType == "future":
            row["assetid"] = simplify_future_native_code(symbol)

        row["type"] = assetType
        row["venue"] = venue
        row["baseAsset"] = item["baseAsset"]
        row["quoteAsset"] = item["quoteAsset"]
        row["marginAsset"] = item["marginAsset"]
        row["quoteAssetPrecision"] = item["quotePrecision"]
        row["baseAssetPrecision"] = item["baseAssetPrecision"]

        if "status" in item:
            row["status"] = item["status"]
        elif "contractStatus" in item:
            row["status"] = item["contractStatus"]
        else:
            row["status"] = "unknown"

        row["underlyingType"] = item.get("underlyingType", None)
        row["contractType"] = item.get("contractType", None)

        # TODO: here I am not supporting the MARKET_LOT_SIZE filter
        for filter in item["filters"]:
            filterType = filter["filterType"]
            if filterType == "LOT_SIZE":
                row["minQty"] = filter["minQty"]
                row["maxQty"] = filter["maxQty"]
                row["lotQty"] = filter["stepSize"]
            elif filterType == "MIN_NOTIONAL":
                row["minNotional"] = filter["notional"]
            elif filterType == "PRICE_FILTER":
                row["tickSize"] = filter["tickSize"]
            elif filterType == "MAX_NUM_ORDERS":
                row["maxNumOrders"] = filter["limit"]
            elif filterType in filters_to_ignore:
                pass
            else:
                logging.warn("ignoring binance filter '{}'".format(filterType))

        rows.append(row)

    df = pd.DataFrame(rows)
    return df


def main():
    qsec.logging.init_logging()

    fn = "binance_exchange-info.json"
    coin_df = parse_binance_spot_exchange_info(fn)

    fn = "binance_usdfut_exchange-info.json"
    futures_df = parse_binance_usdfut_exchange_info(fn, venue="binance_usdfut")

    fn = "binance_coinfut_exchange-info.json"
    coinfut_df = parse_binance_usdfut_exchange_info(fn, venue="binance_coinfut")

    df = pd.concat([coin_df, futures_df, coinfut_df], sort=False)
    df.set_index("assetid", inplace=True, verify_integrity=True)

    outfn = "binance_assets.csv"
    logging.info("writing csv file to '{}'".format(outfn))
    df.to_csv(outfn)


if __name__ == "__main__":
    main()
