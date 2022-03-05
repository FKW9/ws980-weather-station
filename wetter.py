"""
ELV WS980 Weather Station Logger with Graphite and Grafana on an Synology NAS (ds218+).

This script is run every minute on the NAS via the task scheduler.
Requests data from the weather station and sends it to the carbon receiver of graphite.
This data is then visualized with Grafana.

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

WEATHER_HOST     = '192.168.8.14' # IP address of the weather station
WEATHER_PORT     = 45000          # port of the weather station

MAX_RETRIES      = 10             # Retries when requesting data fails


""" Commands which can be sent to the weather station. """
CMD_ACT = b'\xff\xff\x0b\x00\x06\x04\x04\x19'  # get current values
CMD_MIN = b'\xff\xff\x0b\x00\x06\x06\x06\x1d'  # min values
CMD_MAX = b'\xff\xff\x0b\x00\x06\x05\x05\x1b'  # max values


""" Init logger. """
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logHandler = handlers.RotatingFileHandler(
    '/volume1/docker/python/debug.log',
    maxBytes=1e8,
    backupCount=2
)
formatter = logging.Formatter('%(asctime)s %(levelname)s : %(message)s')
logHandler.setFormatter(formatter)
logger.addHandler(logHandler)


"""
Information about the received data.

- Metric name
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
    {'name': 'temperatur.innen',      'start': 7,  'length': 2, 'div': 10,    'format': '>h', 'unit': '°C'  },
    {'name': 'temperatur.aussen',     'start': 10, 'length': 2, 'div': 10,    'format': '>h', 'unit': '°C'  },
    {'name': 'temperatur.taupunkt',   'start': 13, 'length': 2, 'div': 10,    'format': '>h', 'unit': '°C'  },
    {'name': 'temperatur.gefuehlt',   'start': 16, 'length': 2, 'div': 10,    'format': '>h', 'unit': '°C'  },
    {'name': 'temperatur.hitzeIndex', 'start': 19, 'length': 2, 'div': 10,    'format': '>h', 'unit': '°C'  },
    {'name': 'feuchte.innen',         'start': 22, 'length': 1, 'div': 1,     'format': ''  , 'unit': '%'   },
    {'name': 'feuchte.aussen',        'start': 24, 'length': 1, 'div': 1,     'format': ''  , 'unit': '%'   },
    {'name': 'druck.absolut',         'start': 26, 'length': 2, 'div': 10,    'format': '>h', 'unit': 'hPa' },
    {'name': 'druck.relativ',         'start': 29, 'length': 2, 'div': 10,    'format': '>h', 'unit': 'hPa' },
    {'name': 'wind.richtung',         'start': 32, 'length': 2, 'div': 1,     'format': '>h', 'unit': '°'   },
    {'name': 'wind.geschwindigkeit',  'start': 35, 'length': 2, 'div': 10,    'format': '>h', 'unit': 'km/h'},
    {'name': 'wind.boee',             'start': 38, 'length': 2, 'div': 10,    'format': '>h', 'unit': 'km/h'},
    {'name': 'niederschlag.aktuell',  'start': 41, 'length': 4, 'div': 10,    'format': '>I', 'unit': 'mm'  },
    {'name': 'niederschlag.tag',      'start': 46, 'length': 4, 'div': 10,    'format': '>I', 'unit': 'mm'  },
    {'name': 'niederschlag.woche',    'start': 51, 'length': 4, 'div': 10,    'format': '>I', 'unit': 'mm'  },
    {'name': 'niederschlag.monat',    'start': 56, 'length': 4, 'div': 10,    'format': '>I', 'unit': 'mm'  },
    {'name': 'niederschlag.jahr',     'start': 61, 'length': 4, 'div': 10,    'format': '>I', 'unit': 'mm'  },
    {'name': 'niederschlag.gesamt',   'start': 66, 'length': 4, 'div': 10,    'format': '>I', 'unit': 'mm'  },
    {'name': 'licht.aktuell',         'start': 71, 'length': 4, 'div': 10000, 'format': '>I', 'unit': 'w/m²'},  # unrealistic values?!
    {'name': 'licht.uvWert',          'start': 76, 'length': 2, 'div': 10,    'format': '>h', 'unit': 'w/m²'},  # unrealistic values?!
    {'name': 'licht.uvIndex',         'start': 79, 'length': 1, 'div': 1,     'format': ''  , 'unit': ''    }
]


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
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((WEATHER_HOST, WEATHER_PORT))

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

    for retry in range(MAX_RETRIES):
        weather_data = request_data_from_weather_station()

        if weather_data != 0:
            formatted_data = format_data_for_graphite(weather_data)
            if send_data_to_graphite(formatted_data):
                sys.exit(0)

        logging.warning('Retrying... [%s]', retry)
        time.sleep(1)

    logging.error('No success after %s retries. Exiting.', MAX_RETRIES)
    sys.exit(0)
