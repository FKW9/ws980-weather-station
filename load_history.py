"""
ELV WS980 Weather Station Logger with Graphite and Grafana on an Synology NAS (ds218+).

This script loads history data from the weather station into graphite

@author: Florian W
"""
import time
from datetime import datetime

import pandas as pd

from weather import GRAPHITE_METRIC, VALUES, send_data_to_graphite


def load_csv(file: str):
    return pd.read_csv(file, sep='\t', decimal='.', encoding='UTF-16', parse_dates=['Zeit'], date_parser=lambda x: datetime.strptime(x, ' %m/%d/%y %I:%M %p'))


def format_data_for_graphite(data_frame: pd.DataFrame):

    list_of_all_metric_tuples = list()

    for row in range(len(data_frame)):

        list_of_metric_tuples = list()
        unix_timestamp = round(data_frame['Zeit'][row].timestamp())

        for dict in VALUES:
            if dict['csv_name'] != '':
                # [(path, (timestamp, value)), ...]
                list_of_metric_tuples.append(
                    (
                        GRAPHITE_METRIC + dict['name'],
                            (
                                unix_timestamp, float(data_frame[dict['csv_name']][row])
                            )
                    )
                )

        list_of_all_metric_tuples.append(list_of_metric_tuples)

    return list_of_all_metric_tuples


if __name__ == '__main__':
    df = load_csv('data/data.csv')
    formatted_data = format_data_for_graphite(df)

    for metric in formatted_data:
        send_data_to_graphite(metric)
        time.sleep(0.05)
