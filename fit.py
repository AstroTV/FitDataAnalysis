
from ast import arg
from datetime import datetime, timedelta
from statistics import stdev
from typing import Dict, Union, Optional, Tuple

import pandas as pd
import os
import sys

import fitdecode
from math import floor, inf

# The names of the columns we will use in our points DataFrame. For the data we will be getting
# from the FIT data, we use the same name as the field names to make it easier to parse the data.
POINTS_COLUMN_NAMES = ['latitude', 'longitude', 'lap',
                       'altitude', 'timestamp', 'heart_rate', 'cadence', 'speed']

# The names of the columns we will use in our laps DataFrame.
LAPS_COLUMN_NAMES = ['number', 'start_time', 'total_distance', 'total_elapsed_time',
                     'max_speed', 'max_heart_rate', 'avg_heart_rate']


def get_fit_lap_data(frame: fitdecode.records.FitDataMessage) -> Dict[str, Union[float, datetime, timedelta, int]]:
    """Extract some data from a FIT frame representing a lap and return
    it as a dict.
    """

    data: Dict[str, Union[float, datetime, timedelta, int]] = {}

    # Exclude 'number' (lap number) because we don't get that
    for field in LAPS_COLUMN_NAMES[1:]:
        # from the data but rather count it ourselves
        if frame.has_field(field):
            data[field] = frame.get_value(field)

    return data


def get_fit_point_data(frame: fitdecode.records.FitDataMessage) -> Optional[Dict[str, Union[float, int, str, datetime]]]:
    """Extract some data from an FIT frame representing a track point
    and return it as a dict.
    """

    data: Dict[str, Union[float, int, str, datetime]] = {}

    if not (frame.has_field('position_lat') and frame.has_field('position_long')):
        # Frame does not have any latitude or longitude data. We will ignore these frames in order to keep things
        # simple, as we did when parsing the TCX file.
        return None
    elif frame.get_value('position_lat') is None or frame.get_value('position_long') is None:
        return None
    else:
        data['latitude'] = frame.get_value('position_lat') / ((2**32) / 360)
        data['longitude'] = frame.get_value('position_long') / ((2**32) / 360)

    for field in POINTS_COLUMN_NAMES[3:]:
        if frame.has_field(field):
            data[field] = frame.get_value(field)

    return data


def get_dataframes(fname: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Takes the path to a FIT file (as a string) and returns two Pandas
    DataFrames: one containing data about the laps, and one containing
    data about the individual points.
    """

    points_data = []
    laps_data = []
    lap_no = 1
    with fitdecode.FitReader(fname) as fit_file:
        for frame in fit_file:
            if isinstance(frame, fitdecode.records.FitDataMessage):
                if frame.name == 'record':
                    single_point_data = get_fit_point_data(frame)
                    if single_point_data is not None:
                        single_point_data['lap'] = lap_no
                        points_data.append(single_point_data)
                elif frame.name == 'lap':
                    single_lap_data = get_fit_lap_data(frame)
                    single_lap_data['number'] = lap_no
                    laps_data.append(single_lap_data)
                    lap_no += 1

    # Create DataFrames from the data we have collected. If any information is missing from a particular lap or track
    # point, it will show up as a null value or "NaN" in the DataFrame.

    laps_df = pd.DataFrame(laps_data, columns=LAPS_COLUMN_NAMES)
    laps_df.set_index('number', inplace=True)
    points_df = pd.DataFrame(points_data, columns=POINTS_COLUMN_NAMES)

    return laps_df, points_df


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: fit.py <Directory>")
        exit()
    folder = sys.argv[1]
    paths = []
    for file in os.listdir(folder):
        paths.append(folder + "/" + file)

    dist_list = []
    dur_list = []
    gain_list = []
    loss_list = []
    min_dist = inf
    max_dist = 0
    min_gain = inf
    max_gain = 0

    for path in paths:
        laps_df, points_df = get_dataframes(path)
        alt_gain = 0
        alt_loss = 0
        for i in range(1, len(points_df["altitude"])):
            dif = points_df["altitude"][i-1] - points_df["altitude"][i]
            if dif > 0:
                alt_loss += dif
            else:
                alt_gain -= dif
        tot_dist = laps_df["total_distance"].sum()
        tot_duration = laps_df["total_elapsed_time"].sum()/60
        if tot_duration > 70:
            continue
        print("Total distance: {:.2f} in {:.2f} minutes, alt_gain: {:.2f}m, alt_loss = {:.2f}m".format(
            tot_dist, tot_duration, alt_gain, alt_loss))
        dist_list.append(tot_dist)
        dur_list.append(tot_duration)
        if alt_gain > 0:
            gain_list.append(alt_gain)
        loss_list.append(alt_loss)

    avg_dist = sum(dist_list) / len(dist_list)
    avg_dur = sum(dur_list) / len(dur_list)
    avg_gain = sum(gain_list) / len(gain_list)
    avg_loss = sum(loss_list) / len(loss_list)

    print("Average distance : {:.2f} km, Average duration: {}min {:.0f}s".format(
        avg_dist, floor(avg_dur), (avg_dur - floor(avg_dur)) * 60))
    print("Average alt_gain : {:.2f} m, Average alt_loss: {:.2f} m".format(
        avg_gain, avg_loss))
    print("Range distance: {:.2f}m - {:.2f}m ({:.2f}m), Range alt_gain: {:.2f}m - {:.2f}m ({:.2f}m)".format(
        min(dist_list), max(dist_list), stdev(dist_list), min(gain_list), max(gain_list), stdev(gain_list)))
    print("Range duration: {}min {:.0f}s - {}min {:.0f}s ({}min {:.0f}s)".format(floor(min(dur_list)), (min(dur_list) - floor(min(dur_list))) * 60,
          floor(max(dur_list)), (max(dur_list) - floor(max(dur_list))) * 60, floor(stdev(dur_list)), (stdev(dur_list) - floor(stdev(dur_list))) * 60))
