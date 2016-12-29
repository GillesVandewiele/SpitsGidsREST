from pymongo import MongoClient


class SpitsGidsMongoDAO(object):

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.db = self.load_data_from_db()

    def load_data_from_db(self):
        return MongoClient(self.host, self.port)['SpitsGids']

    def insert_log(self, log):
        return self.db['logs'].insert_one(log)

    def find_log_by_vehicle_and_querytime(self, vehicle, querytime):
        pass

    def insert_logs(self, logs):
        return self.db['logs'].insert_many(logs)

    def insert_feature_vector(self, log, feature_vector):
        log_id = self.db['logs'].find(log)["_id"]
        if log_id is None:
            log_id = self.insert_log(log)["inserted_id"]
        feature_vector['log_id'] = log_id
        return self.db['features'].insert_one(feature_vector)

    def insert_feature_vectors(self, logs, feature_vectors):
        for log, feature_vector in zip(logs, feature_vectors):
            log_id = self.db['logs'].find(log)["_id"]
            if log_id is None:
                log_id = self.insert_log(log)["inserted_id"]
            feature_vector['log_id'] = log_id
        return self.db['features'].insert_many(feature_vectors)
