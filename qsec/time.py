import datetime as dt
import pytz
import time
from typing import Optional, Union


def short_fmt(date: dt.date) -> str:
    return dt.datetime.strftime(date, "%Y%m%d")


def date_to_datetime(
    d: dt.date,
    hour: Optional[int] = 0,
    minute: Optional[int] = 0,
    second: Optional[int] = 0,
) -> dt.datetime:
    return dt.datetime(d.year, d.month, d.day, hour, minute, second).replace(
        tzinfo=pytz.UTC
    )


def date_range(lower: Union[dt.date, str], upper: Union[dt.date, str]):
    if isinstance(lower, str):
        lower = to_date(lower)
    if isinstance(upper, str):
        upper = to_date(upper)
    while lower < upper:
        yield lower
        lower += dt.timedelta(days=1)


def dates_in_range(lower: dt.date, upper: dt.date) -> list:
    dates = []
    date = lower
    while date < upper:
        dates.append(date)
        date += dt.timedelta(days=1)
    return dates


def to_date(s: str) -> dt.date:
    fmt = None
    if len(s) == 8:
        fmt = "%Y%m%d"
    elif len(s) == 10:
        fmt = "%Y-%m-%d"
    else:
        raise Exception("unknown date format for string '{}'".format(s))
    return dt.datetime.strptime(s, fmt).replace(tzinfo=pytz.UTC).date()


# Convert a string like "2021-01-31 22:00:00" to a UTC datetime object
def str_to_datetime_utc(s: str) -> dt.datetime:
    fmt = None
    if len(s) == 8:
        fmt = "%Y%m%d"
    elif len(s) == 10:
        fmt = "%Y-%m-%d"
    elif len(s) == 19:
        fmt = "%Y-%m-%d %H:%M:%S"
    else:
        raise Exception("unknown timestamp format for string '{}'".format(s))
    return dt.datetime.strptime(s, fmt).replace(tzinfo=pytz.UTC)


# Convert a datetime object to epoch milliseconds
def datetime_to_epoch_ms(ts: dt.datetime):
    return int(ts.timestamp() * 1000.0)


# Get current UTC time in milliseconds since epoch
def now_epoch_ms():
    return int(time.time() * 1000.0)


def epoch_ms_to_str(now):
    ts = dt.datetime.fromtimestamp(now / 1000)
    return ts.strftime("%Y-%m-%d %H:%M:%S.{:0>3d}".format(int(ts.microsecond / 1000)))


def epoch_ms_to_dt(epoch_ms) -> dt.datetime:
    return dt.datetime.fromtimestamp(epoch_ms / 1000.0)
