
from dateutil.parser import parse
import datetime


# utility functions

def _datetime_from_utc(date_utc_str):
    return parse(date_utc_str)


def _unix_time_millis_from_datetime(dt):
    if type(dt) not in [datetime.date, datetime.datetime]:
        raise ValueError('Accepting only datetime.date or datetime.datetime')

    epoch = datetime.datetime.utcfromtimestamp(0)
    if not isinstance(dt, datetime.datetime):
        dt = datetime.datetime.combine(dt, datetime.time())

    return int((dt.replace(tzinfo=None) - epoch).total_seconds() * 1000)


def _convert_to_datetime(dt):
    if type(dt) in [datetime.date, datetime.datetime]:
        return dt
    elif isinstance(dt, int):
        # TODO WRONG LOGIC if timestamp is not in million seconds
        return datetime.datetime.utcfromtimestamp(dt / 1000.0)
    elif type(dt) in [unicode, str]:
        return _datetime_from_utc(dt)
    else:
        raise ValueError('Cannot convert {} to datetime'.format(dt))
