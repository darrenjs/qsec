import sys


class EasyError(Exception):
    def __init__(self, message: str):
        super().__init__(message)


def main(fn):
    try:
        fn()
    except EasyError as err:
        print(f"{err}")
        sys.exit(1)
