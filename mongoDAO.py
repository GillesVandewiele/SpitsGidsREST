from pymongo import MongoClient
import pandas as pd
from xgb import XGBModel

from feature_extractor import extract_basic_features


class SpitsGidsMongoDAO(object):

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.db = self.load_data_from_db()

    def load_data_from_db(self):
        """
        Load the SpitsGids database
        :return: the database
        """
        return MongoClient(self.host, self.port)['SpitsGids']

    def insert_log(self, log):
        """
        Insert a log entry
        :param log: dict containing columns of table as keys and corresponding values
        :return: result object
        """
        return self.db['logs'].insert_one(log)

    def log_exists(self, vehicle, querytime):
        """
        Check if a certain log (same vehicle and querytime) already exists in database
        :param vehicle:  the vehicle of the log
        :param querytime:  the querytime of the log
        :return: a boolean indicating if the log is already in the database
        """
        return self.db['logs'].find({"vehicle_id": vehicle, "querytime": querytime}).count() > 0

    def insert_logs(self, logs):
        """
        Insert multiple log entries
        :param logs: list of logs (dict)
        :return: result object
        """
        return self.db['logs'].insert_many(logs)

    def insert_feature_vector(self, log, feature_vector):
        """
        Inserts a feature vector, that is calculated from a certain log (linked by _id)
        :param log: the log from which the feature_vector is calculated
        :param feature_vector: the vector of features
        :return: result object
        """
        log = self.db['logs'].find_one(log)
        if log is None: raise

        log_id = log["_id"]
        if log_id is None:
            log_id = self.insert_log(log)["inserted_id"]
        feature_vector['log_id'] = log_id
        return self.db['features'].insert_one(feature_vector)

    def process_unprocessed_logs(self):
        """
        Filter out all logs with processed equal to False and calculate their corresponding feature vector. Then put the
        processed variable on True and insert the feature vector in the database
        :return: nothing
        """
        unprocessed_logs = self.db['logs'].find({'processed': False})
        for log in unprocessed_logs:
            log.pop('empty', None)
            processed_log, feature_vector = extract_basic_features(log, self)
            self.insert_feature_vector(processed_log, feature_vector)
            self.db['logs'].update_one(log, {"$set": {'processed': True}})

    def count_records_per_table(self):
        print('Number of logs:', self.db['logs'].find({}).count())
        print('Number of feature vectors:', self.db['logs'].find({}).count())

    def clean_logs_table(self):
        self.db['logs'].remove({})

    def clean_features_table(self):
        self.db['features'].remove({})

    def load_stations_table(self, stations_csv):
        """
        Parse csv file containing information about stations and load them in the database
        :param stations_csv: csv file containing the information about stations
        :return: nothing
        """
        self.db['stations'].remove({})
        stations_df = pd.read_csv(stations_csv)
        stations_df['ID'] = stations_df['URI'].apply(lambda x: x.split('/')[-1])
        self.db['stations'].insert_many(stations_df[['ID', 'name', 'country-code', 'longitude',
                                                     'latitude', 'avg_stop_times']].to_dict(orient='records'))

    def get_station_info_by_id(self, station_id):
        station_cursor = self.db['stations'].find({'ID': station_id})
        if station_cursor.count() == 0:
            return None
        else:
            return station_cursor[0]