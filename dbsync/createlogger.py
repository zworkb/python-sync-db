import logging
from sys import stdout


def create_logger(name):
    logger = logging.getLogger(name)
    # logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(stdout)
    handler.setFormatter(logging.Formatter("%(levelname)s : %(name)s :: %(funcName)s() : %(message)s"))
    logger.addHandler(handler)
    return logger
