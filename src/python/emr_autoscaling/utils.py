from datetime import datetime

from src.python.pytz import timezone
from logging import getLogger, Formatter, StreamHandler


def get_logger(name, log_level='INFO'):
    logger = getLogger(name)
    logger.setLevel(log_level)

    handler = StreamHandler()
    handler.setFormatter(Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    handler.setLevel(log_level)

    logger.addHandler(handler)

    return logger


def create_berlin_time(input_time):
    time_zone = timezone('Europe/Berlin')

    time_offset = int(
        (datetime
         .now(time_zone)
         .utcoffset()
         .total_seconds()) / (60 * 60)
    )

    return input_time \
        .replace(hour=input_time.hour - time_offset,
                 minute=input_time.minute,
                 second=input_time.second,
                 microsecond=input_time.microsecond)

