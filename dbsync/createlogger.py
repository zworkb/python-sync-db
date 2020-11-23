import logging
from sys import stdout


def create_logger(name, level: int = logging.INFO):
    from dbsync.core import mode
    logger = logging.getLogger(name)

    logger.setLevel(level)
    formatter = logging.Formatter(f"[{mode}]%(levelname)s[%(module)s][%(lineno)d] : %(name)s :: %(funcName)s()  : %(message)s")

    handlers = [logging.StreamHandler(stdout)]

    try:
        handlers.append(logging.FileHandler("/tmp/woodmaster.log", "a"))
    except FileNotFoundError:
        pass

    try:
        handlers.append(logging.FileHandler("/sdcard/woodmaster/woodmaster.log", "a"))
    except Exception:
        pass

    for handler in handlers:
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger
