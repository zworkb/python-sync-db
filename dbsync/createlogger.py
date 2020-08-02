import logging
from sys import stdout


def create_logger(name, level: int = logging.INFO):
    from dbsync.core import mode
    logger = logging.getLogger(name)

    logger.setLevel(level)
    formatter = logging.Formatter(f"[{mode}]%(levelname)s[%(module)s][%(lineno)d] : %(name)s :: %(funcName)s()  : %(message)s")

    for handler in [logging.StreamHandler(stdout), logging.FileHandler("/tmp/woodmaster.log", "a")]:
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger
