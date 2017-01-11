import pandas as pd
import numpy as np
import re
from dateutil.parser import parse


def extract_features_prediction(departureTime, vehicle, from_id, to_id, mongoDAO):
    # Vehicle features
    pattern = re.compile("^([A-Z]+)[0-9]+$")
    m = pattern.match(vehicle)
    vehicle_type = None
    if m is not None:
        vehicle_type = m.group(1)

    # Time features
    week_day_mapping = {0: 'Monday', 1: 'Tuesday', 2: 'Wednesday', 3: 'Thursday', 4: 'Friday', 5: 'Saturday',
                        6: 'Sunday'}
    parsed_query_time = departureTime
    week_day = week_day_mapping[parsed_query_time.weekday()]
    midnight = parsed_query_time.replace(hour=0, minute=0, second=0, microsecond=0)
    seconds_since_midnight = (parsed_query_time - midnight).seconds
    month = parsed_query_time.month
    hour = parsed_query_time.hour
    commute_days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
    print(type(parsed_query_time), parsed_query_time.tzinfo)
    if parsed_query_time.tzinfo is not None:
        timezone_offset = parsed_query_time.tzinfo._offset
        hours_offset, remainder = divmod(timezone_offset.seconds, 3600)
    else:
        hours_offset = 0

    if 6 < (hour - hours_offset + 1) < 10 and week_day in commute_days:
        morning_commute = 1
    else:
        morning_commute = 0

    if 15 < (hour - hours_offset + 1) < 19 and week_day in commute_days:
        evening_commute = 1
    else:
        evening_commute = 0

    from_station = mongoDAO.get_station_info_by_id(from_id)
    if from_station is None:
        avg_stop_time_from, from_lat, from_lng = None, None, None
    else:
        avg_stop_time_from, from_lat, from_lng = from_station['avg_stop_times'], from_station['latitude'], \
                                                 from_station['longitude']

    to_station = mongoDAO.get_station_info_by_id(to_id)
    if to_station is None:
        avg_stop_time_to, to_lat, to_lng = None, None, None
    else:
        avg_stop_time_to, to_lat, to_lng = to_station['avg_stop_times'], to_station['latitude'], to_station['longitude']

    return { 'vehicle_id': vehicle, 'vehicle_type': vehicle_type, 'week_day': week_day,
             'seconds_since_midnight': seconds_since_midnight, 'month': month, 'morning_jam': morning_commute,
             'evening_jam': evening_commute, 'avg_stop_time_from': avg_stop_time_from,
             'avg_stop_time_to': avg_stop_time_to,
             'from_lat': from_lat, 'from_lng': from_lng, 'to_lat': to_lat, 'to_lng': to_lng }

def extract_basic_features(log, mongoDAO):
    # Vehicle features
    pattern = re.compile("^([A-Z]+)[0-9]+$")
    m = pattern.match(log['vehicle_id'])
    vehicle_type = None
    if m is not None:
        vehicle_type = m.group(1)

    # Time features
    week_day_mapping = {0: 'Monday', 1: 'Tuesday', 2: 'Wednesday', 3: 'Thursday', 4: 'Friday', 5: 'Saturday',
                        6: 'Sunday'}
    parsed_query_time = log['querytime']
    week_day = week_day_mapping[parsed_query_time.weekday()]
    midnight = parsed_query_time.replace(hour=0, minute=0, second=0, microsecond=0)
    seconds_since_midnight = (parsed_query_time - midnight).seconds
    month = parsed_query_time.month
    hour = parsed_query_time.hour
    commute_days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
    if parsed_query_time.tzinfo is not None:
        timezone_offset = parsed_query_time.tzinfo._offset
        hours_offset, remainder = divmod(timezone_offset.seconds, 3600)
    else:
        hours_offset = 0

    if 6 < (hour - hours_offset + 1) < 10 and week_day in commute_days:
        morning_commute = 1
    else:
        morning_commute = 0

    if 15 < (hour - hours_offset + 1) < 19 and week_day in commute_days:
        evening_commute = 1
    else:
        evening_commute = 0

    # Station features
    from_id = log['from_id']
    to_id = log['to_id']

    from_station = mongoDAO.get_station_info_by_id(from_id)
    if from_station is None:
        avg_stop_time_from, from_lat, from_lng = None, None, None
    else:
        avg_stop_time_from, from_lat, from_lng = from_station['avg_stop_times'], from_station['latitude'], \
                                                 from_station['longitude']

    to_station = mongoDAO.get_station_info_by_id(to_id)
    if to_station is None:
        avg_stop_time_to, to_lat, to_lng = None, None, None
    else:
        avg_stop_time_to, to_lat, to_lng = to_station['avg_stop_times'], to_station['latitude'], to_station['longitude']

    return log, {'vehicle_id': log['vehicle_id'], 'vehicle_type': vehicle_type, 'week_day': week_day,
                 'seconds_since_midnight': seconds_since_midnight, 'month': month,  'occupancy': log['occupancy'],
                 'morning_jam': morning_commute, 'evening_jam': evening_commute,
                 'avg_stop_time_from': avg_stop_time_from, 'avg_stop_time_to': avg_stop_time_to,
                 'from_lat': from_lat, 'from_lng': from_lng, 'to_lat': to_lat, 'to_lng': to_lng
                }


# TODO: extract features from parameters passed along with predict functions
"""
    departureTime = request.args.get('departureTime')
    vehicle = request.args.get('vehicle')
    _from = request.args.get('from')
    _to = request.args.get('to')
"""