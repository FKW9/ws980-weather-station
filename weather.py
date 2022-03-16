"""
ELV WS980 Weather Station Logger with Graphite and Grafana on an Synology NAS (ds218+).

This script is run every minute on the NAS via the task scheduler.
Requests data from the weather station and sends it to the carbon receiver of graphite.
This data is then visualized with Grafana.

UPDATE:
Since my NAS has some kind of Problem with its task scheduler, after some time the python process just hangs up and does not end, thus thus we can't get new data.
To solve this issue, i just run a while(True) loop and wait 60 seconds to request new data.
This script should be executed on each startup of the NAS...

@author: Florian W
"""
import logging
import logging.handlers as handlers
import pickle
import socket
import struct
import sys
import time


""" Configuration options. """
GRAPHITE_HOST    = '192.168.8.42' # IP address of the NAS
GRAPHITE_PORT    = 2004           # port for carbon receiver, 2004 is for pickled data
GRAPHITE_TIMEOUT = 2
GRAPHITE_METRIC  = 'wetter.'      # metric header

WEATHER_HOST     = '192.168.8.55' # IP address of the weather station
WEATHER_PORT     = 45000          # port of the weather station
WEATHER_INTERVAL = 60

MAX_RETRIES      = 10             # Retries when requesting data fails


""" Commands which can be sent to the weather station. """
CMD_ACT = b'\xff\xff\x0b\x00\x06\x04\x04\x19'  # get current values
CMD_MIN = b'\xff\xff\x0b\x00\x06\x06\x06\x1d'  # min values
CMD_MAX = b'\xff\xff\x0b\x00\x06\x05\x05\x1b'  # max values


"""
Information about the received data.

- Metric name
- Name of the metric in the CSV which can be generated from the WeatherSmartIP Software to see historical data
- Start index of value in bytearray (received from weather station)
- Length of the data (1, 2 or 4 Bytes)
- Divisor of the value
- Datatype, either
  - ">h" = Big Endian Short        -32768..32767
  - ">I" = Big Endian Unsigned Int      0..4294967295
  - "" for None = 1                     0..255
- Unit (not necessary)
"""
VALUES = [
    {'name': 'temperatur.innen',      'csv_name': 'Innentemperatur(°C)',           'start': 7,  'length': 2, 'div': 10,    'format': '>h', 'unit': '°C'  },
    {'name': 'temperatur.aussen',     'csv_name': 'Außentemperatur(°C)',           'start': 10, 'length': 2, 'div': 10,    'format': '>h', 'unit': '°C'  },
    {'name': 'temperatur.taupunkt',   'csv_name': 'Taupunkt(°C)',                  'start': 13, 'length': 2, 'div': 10,    'format': '>h', 'unit': '°C'  },
    {'name': 'temperatur.gefuehlt',   'csv_name': 'Gefühlte Temperatur(°C)',       'start': 16, 'length': 2, 'div': 10,    'format': '>h', 'unit': '°C'  },
    {'name': 'temperatur.hitzeIndex', 'csv_name': '',                              'start': 19, 'length': 2, 'div': 10,    'format': '>h', 'unit': '°C'  },
    {'name': 'feuchte.innen',         'csv_name': 'Innenluftfeuchtigkeit(%)',      'start': 22, 'length': 1, 'div': 1,     'format': ''  , 'unit': '%'   },
    {'name': 'feuchte.aussen',        'csv_name': 'Außenluftfeuchtigkeit(%)',      'start': 24, 'length': 1, 'div': 1,     'format': ''  , 'unit': '%'   },
    {'name': 'druck.absolut',         'csv_name': 'Absoluter Luftdruck(hPa)',      'start': 26, 'length': 2, 'div': 10,    'format': '>h', 'unit': 'hPa' },
    {'name': 'druck.relativ',         'csv_name': 'Relativer Luftdruck(hPa)',      'start': 29, 'length': 2, 'div': 10,    'format': '>h', 'unit': 'hPa' },
    {'name': 'wind.richtung',         'csv_name': 'Windrichtung',                  'start': 32, 'length': 2, 'div': 1,     'format': '>h', 'unit': '°'   },
    {'name': 'wind.geschwindigkeit',  'csv_name': 'Wind(km/h)',                    'start': 35, 'length': 2, 'div': 10,    'format': '>h', 'unit': 'km/h'},
    {'name': 'wind.boee',             'csv_name': 'Windböe(km/h)',                 'start': 38, 'length': 2, 'div': 10,    'format': '>h', 'unit': 'km/h'},
    {'name': 'niederschlag.aktuell',  'csv_name': '',                              'start': 41, 'length': 4, 'div': 10,    'format': '>I', 'unit': 'mm'  },
    {'name': 'niederschlag.tag',      'csv_name': '24-Stunden-Niederschlag(mm)',   'start': 46, 'length': 4, 'div': 10,    'format': '>I', 'unit': 'mm'  },
    {'name': 'niederschlag.woche',    'csv_name': 'WöchentlicherNiederschlag(mm)', 'start': 51, 'length': 4, 'div': 10,    'format': '>I', 'unit': 'mm'  },
    {'name': 'niederschlag.monat',    'csv_name': 'Monatlicher Niederschlag(mm)',  'start': 56, 'length': 4, 'div': 10,    'format': '>I', 'unit': 'mm'  },
    {'name': 'niederschlag.jahr',     'csv_name': 'Jahr Niederschlag(mm)',         'start': 61, 'length': 4, 'div': 10,    'format': '>I', 'unit': 'mm'  },
    {'name': 'niederschlag.gesamt',   'csv_name': 'Gesamter Niederschlag(mm)',     'start': 66, 'length': 4, 'div': 10,    'format': '>I', 'unit': 'mm'  },
    {'name': 'licht.aktuell',         'csv_name': 'Beleuchtung(w/m2)',             'start': 71, 'length': 4, 'div': 10000, 'format': '>I', 'unit': 'w/m²'},  # unrealistic values?!
    {'name': 'licht.uvWert',          'csv_name': '',                              'start': 76, 'length': 2, 'div': 10,    'format': '>h', 'unit': 'w/m²'},  # unrealistic values?!
    {'name': 'licht.uvIndex',         'csv_name': 'UV-Index',                      'start': 79, 'length': 1, 'div': 1,     'format': ''  , 'unit': ''    }
]


def init_logger(file: str = '/volume1/docker/python/debug.log'):
    """ Init logger. """
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    logHandler = handlers.RotatingFileHandler(
        file,
        maxBytes=1e8,
        backupCount=2
    )
    formatter = logging.Formatter('%(asctime)s %(funcName)s %(lineno)d %(levelname)s : %(message)s')
    logHandler.setFormatter(formatter)
    logger.addHandler(logHandler)


def check_crc(data):
    """
    Check CRC Value of received data.

    Parameters
    ----------
    data : bytes
        received data

    Returns
    -------
    bool
        crc correct?
    """
    if data != 0:
        crc = 0
        for i in data[2:81]:
            crc += i

        return data[81] == (crc & 255)

    return False


def bytes_to_float(data, start, length, div, fmt):
    """
    Convert the byte values of the given metric to a float.

    Parameters
    ----------
    data : bytes
        received data
    start : int
        start index of byte values
    length : int
        how much bytes
    div : int
        divisor for byte value
    fmt : str
        unpack format for struct.unpack()

    Returns
    -------
    float
        converted value
    """
    bytes_value = data[start:start + length]

    if length == 1:
        return int(bytes_value.hex(), 16) / div

    return struct.unpack(fmt, bytes_value)[0] / div


def request_data_from_weather_station():
    """
    Send a command to the weather station to get current values.

    Returns
    -------
    bytes
        received data, 0 if error occurred
    """
    # sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # sock.connect((WEATHER_HOST, WEATHER_PORT))
    # sock.set

    sock = socket.create_connection((WEATHER_HOST, WEATHER_PORT), GRAPHITE_TIMEOUT)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    data = 0
    try:
        sock.send(CMD_ACT)
        data = sock.recv(1024)
    except:
        logging.error('Error getting data from weather station!')
    finally:
        sock.close()
    if check_crc(data):
        return data

    logging.error('CRC failed! \r\n Data: %s', data)
    return 0


def format_data_for_graphite(data):
    """
    Format all data into a list of metric tuples for the carbon receiver.

    Parameters
    ----------
    data : bytes
        received data

    Returns
    -------
    list
        [(path, (timestamp, value)), ...]
    """
    # [(path, (timestamp, value)), ...]
    list_of_metric_tuples = list()
    current_time = int(time.time())
    for value in VALUES:
        list_of_metric_tuples.append(
            (
                GRAPHITE_METRIC + value['name'],
                    (
                        current_time, bytes_to_float(data, value['start'], value['length'], value['div'], value['format'])
                    )
            )
        )

    return list_of_metric_tuples


def send_data_to_graphite(list_of_metric_tuples):
    """
    Send the data, which was converted to a certain format, to graphites carbon receiver.

    See Description here: https://graphite.readthedocs.io/en/latest/feeding-carbon.html#the-pickle-protocol

    Parameters
    ----------
    list_of_metric_tuples : list
        [(path, (timestamp, value)), ...], our measurement values

    Returns
    -------
    bool
        success?
    """
    payload = pickle.dumps(list_of_metric_tuples, protocol=2)
    header  = struct.pack("!L", len(payload))
    message = header + payload

    sock = socket.create_connection((GRAPHITE_HOST, GRAPHITE_PORT), GRAPHITE_TIMEOUT)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    success = False
    try:
        sock.sendall(message)
        logging.info('Data successfully sent to graphite.')
        success = True
    except:
        logging.error('Error sending data!\n%s', sys.exc_info())
    finally:
        sock.close()

    return success


if __name__ == '__main__':

    init_logger()

    while True:
        for retry in range(MAX_RETRIES):
            try:
                weather_data = request_data_from_weather_station()

                if weather_data != 0:
                    formatted_data = format_data_for_graphite(weather_data)

                    result = send_data_to_graphite(formatted_data)
                    if result is False:
                        logging.error('Error sending data to graphite.')
                        logging.warning('Retrying... [%s]', retry)
                        time.sleep(1)
                    else:
                        break
            except:
                logging.critical('Unexpected error! %s.', sys.exc_info())

        time.sleep(WEATHER_INTERVAL)
