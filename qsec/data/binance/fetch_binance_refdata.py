import requests
import json
import logging
import sys

import qsec.logging


spot_api = "https://api.binance.com"
spot_path = "/api/v3/exchangeInfo"

usdfut_api = "https://fapi.binance.com"
usdfut_path = "/fapi/v1/exchangeInfo"

coinfut_api = "https://dapi.binance.com"
coinfut_path = "/dapi/v1/exchangeInfo"


def perform_http_request(api, path):
    url = f"{api}{path}"
    logging.info("making HTTP GET request: {}".format(url))
    reply = requests.get(url, params=None)

    if reply.status_code != 200:
        raise Exception("http request failed: {}".format(reply.status_code))
    return reply.text


def main():
    qsec.logging.init_logging()

    reply = perform_http_request(usdfut_api, usdfut_path)
    fn = "binance_usdfut_exchange-info.json"
    logging.info("writing to file '{}'".format(fn))
    with open(fn, "w") as f:
        f.write(reply)

    reply = perform_http_request(coinfut_api, coinfut_path)
    fn = "binance_coinfut_exchange-info.json"
    logging.info("writing to file '{}'".format(fn))
    with open(fn, "w") as f:
        f.write(reply)

    reply = perform_http_request(spot_api, spot_path)
    fn = "binance_exchange-info.json"
    logging.info("writing to file '{}'".format(fn))
    with open(fn, "w") as f:
        f.write(reply)


if __name__ == "__main__":
    main()
