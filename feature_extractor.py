import pandas as pd
import numpy as np
import re
from dateutil.parser import parse


def extract_basic_features(log):
    # Vehicle features
    pattern = re.compile("^([A-Z]+)[0-9]+$")
    vehicle_type = pattern.match(log['vehicle_id']).group(1)

    # Time features
    week_day_mapping = {0: 'Monday', 1: 'Tuesday', 2: 'Wednesday', 3: 'Thursday', 4: 'Friday', 5: 'Saturday',
                        6: 'Sunday'}
    parsed_query_time = parse(log['querytime'])
    week_day = week_day_mapping[parsed_query_time.weekday()]
    midnight = parsed_query_time.replace(hour=0, minute=0, second=0, microsecond=0)
    minutes_since_midnight = (parsed_query_time - midnight).seconds/60
    month = parsed_query_time.month


    return log, {'vehicle_id': log['vehicle_id'], 'vehicle_type': log['type'], 'week_day': week_day,
                 'minutes_since_midnight': minutes_since_midnight, 'month': month}