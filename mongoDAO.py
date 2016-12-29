from pymongo import MongoClient
import pandas as pd

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

    def insert_feature_vectors(self, logs, feature_vectors):
        """
        Inserts multiple feature vectors, that are calculated from certain logs (linked by _id)
        :param logs: list of logs
        :param feature_vectors: list of feature_vectors
        :return: result object
        """
        for log, feature_vector in zip(logs, feature_vectors):
            log_id = self.db['logs'].find(log)["_id"]
            if log_id is None:
                log_id = self.insert_log(log)["inserted_id"]
            feature_vector['log_id'] = log_id
        return self.db['features'].insert_many(feature_vectors)

    def process_unprocessed_logs(self):
        """
        Filter out all logs with processed equal to False and calculate their corresponding feature vector. Then put the
        processed variable on True and insert the feature vector in the database
        :return: nothing
        """
        unprocessed_logs = self.db['logs'].find({'processed': False})
        for log in unprocessed_logs:
            log.pop('empty', None)
            processed_log, feature_vector = extract_basic_features(log)
            self.insert_feature_vector(processed_log, feature_vector)
            self.db['logs'].update_one(log, {"$set": {'processed': True}})

    def load_stations_table(self, stations_csv):
        """
        Parse csv file containing information about stations and load them in the database
        :param stations_csv: csv file containing the information about stations
        :return: nothing
        """
        if self.db['stations'].find({}).count() == 0:
            stations_df = pd.read_csv(stations_csv)
            self.db['stations'].insert_many(stations_df[['station', 'week', 'zaterdag', 'zondag',
                                                           'lat', 'lng', 'station_link']].to_dict(orient='records'))