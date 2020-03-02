"""
.. module:: dbsync.logs
   :synopsis: Logging facilities for the library.
"""

import logging


#: All the library loggers
loggers = set()


log_handler = None


def get_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    loggers.add(logger)
    if log_handler is not None:
        logger.addHandler(log_handler)
    return logger


def set_log_target(fo):
    """
    Set a stream as target for dbsync's logging. If a string is given,
    it will be considered to be a path to a file.
    """
    global log_handler
    if log_handler is None:
        log_handler = logging.FileHandler(fo) if isinstance(fo, str) \
            else logging.StreamHandler(fo)
        log_handler.setLevel(logging.WARNING)
        log_handler.setFormatter(
            logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
        for logger in loggers:
            logger.addHandler(log_handler)
