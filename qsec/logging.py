import logging
import os
import sys

def log_script_start(logger=logging.getLogger()):
    logger.info("======================================================================")
    logger.info("bin : {} ".format(os.path.basename(sys.argv[0])))
    logger.info("args: {} ".format(sys.argv[1:]))
    logger.info("cwd : {} ".format(os.getcwd()))
    logger.info("pid : {} ".format(os.getpid()))
    logger.info("ppid: {} ".format(os.getppid()))
    logger.info("======================================================================")


def init_logging(debug=False, preamble=True):
    logger = logging.getLogger()
    if debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    ch = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("%(asctime)s.%(msecs)03d;%(levelname)s;%(message)s",
                                  "%Y-%m-%d %H:%M:%S")
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    if preamble:
        log_script_start()
