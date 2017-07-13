import logging
import sys


def cli_logger(level=logging.NOTSET):
    logger = logging.getLogger()

    if logger.hasHandlers():
        return logger

    logger.setLevel(level)

    ch = logging.StreamHandler(sys.stderr)
    format = logging.Formatter("[%(asctime)s][%(levelname)s][%(name)s]\t%(message)s")
    ch.setFormatter(format)
    ch.setLevel(logging.NOTSET)
    logger.addHandler(ch)

    return logger