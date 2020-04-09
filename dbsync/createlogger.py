import logging
from sys import stdout


def create_logger(name, level: int = logging.INFO):
    logger = logging.getLogger(name)

    logger.setLevel(level)
    handler = logging.StreamHandler(stdout)
    handler.setFormatter(logging.Formatter("%(levelname)s[%(module)s][%(lineno)d] : %(name)s :: %(funcName)s()  : %(message)s"))
    logger.addHandler(handler)
    return logger
