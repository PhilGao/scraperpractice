import datetime
import time
from threading import current_thread

def convert_to1970s(dt):
    '''
    convert a datetime into micro-seconds since 1970-1-1
    :param dtt datetime
    :return: ms int
    '''
    try:
        return int(dt.timestamp() * 1000)
    except ValueError:
        print('Please check the year month day hour minute second ms is valid')
    except TypeError:
        print('Please check the year month day hour minute second ms is valid')
    except Exception as e:
        print('threading :',current_thread(),e)

    #return int(time.mktime(dt.timetuple()) * 1000)


def convert_from1970s(ms):
    return datetime.datetime.fromtimestamp(ms/1000).strftime('%Y-%m-%d %H:%M:%S')