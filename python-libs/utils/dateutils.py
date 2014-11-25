import datetime, time
from datetime import datetime

fmt = '%Y-%m-%d %H:%M:%S'

def current_time():
    return datetime.now().strftime(fmt)


def diff_data_minute( d1 ):

    d2 = current_time()
    d1 = datetime.strptime(d1, fmt)
    d2 = datetime.strptime(d2, fmt)

    d1_ts = time.mktime(d1.timetuple())
    d2_ts = time.mktime(d2.timetuple())
    return int(d2_ts-d1_ts) / 60

def clear_coding( txt, codif='utf-8'):
    try:
        return normalize('NFKD', txt.decode(codif)).encode('ASCII','ignore')
    except Exception as ex:
        return txt