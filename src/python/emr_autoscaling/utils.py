from logging import getLogger, Formatter, StreamHandler


def get_logger(name, log_level='INFO'):
    logger = getLogger(name)
    logger.setLevel(log_level)

    handler = StreamHandler()
    handler.setFormatter(Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    handler.setLevel(log_level)

    logger.addHandler(handler)

    return logger
