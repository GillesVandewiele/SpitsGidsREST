import time
from flask import Flask, request, jsonify
from flask_restful import Api
import threading

from datascraper import parse_logs
from feature_extractor import extract_features_prediction
from mongoDAO import SpitsGidsMongoDAO
from dateutil.parser import parse
import pandas as pd
import numpy as np

from xgb import XGBModel


class SpitsGidsServer:

    def __init__(self, host, port, mongoDAO):
        self.app = Flask('SpitsGidsServer')
        api = Api(self.app)
        self.host = host
        self.port = port
        self.xgb_clf = None
        self.feature_cols = None
        self.categorical_cols = ['week_day', 'vehicle_type', 'vehicle_id']
        self.label_col = None
        self.data = None
        self.mongoDAO = mongoDAO
        self.start_server(self.host, self.port)

    def load_data(self):
        self.mongoDAO.process_unprocessed_logs()
        feature_vectors = self.mongoDAO.get_feature_vectors()
        self.label_col = 'occupancy'
        self.data = pd.DataFrame(feature_vectors)
        print(self.data['week_day'])
        self.data = pd.get_dummies(self.data, columns=self.categorical_cols)
        print(self.data)
        self.data = self.data.drop(['_id', 'log_id'], axis=1)
        self.feature_cols = list(set(self.data.columns) - {'occupancy'})
        self.xgb_clf = XGBModel(self.data, self.feature_cols, self.label_col)

    def train_model(self):
        self.load_data()
        ParamOpt(self.xgb_clf).start()
        self.xgb_clf.construct_model()
        #  time.strftime('%y%m%d%H%M%S.model')
        #self.xgb_clf.load_model('170115175919.model')

    def start_server(self, host, port, local=False):
        self.train_model()
        self.add_routes()
        if not local:
            self.app.run(host=host, port=port)
        else:
            self.app.run(debug=True, port=host)

    def predict(self, departure_time, vehicle, _from, _to):
        df = pd.DataFrame([extract_features_prediction(departure_time, vehicle, _from, _to, self.mongoDAO)])
        df = pd.get_dummies(df, self.categorical_cols)
        dummies_frame = pd.get_dummies(self.data)
        df = df.reindex(columns=dummies_frame.columns, fill_value=0)
        print(df)
        df = df[self.feature_cols]
        return self.xgb_clf.model.predict_proba(df)[0]

    def add_routes(self):

        @self.app.route('/predict_by_vehicle', methods=['GET'])
        def predict_by_vehicle():
            """
            Predicts the occupancy level, given a certain vehicle identifier and its departure time
            :return: the predicted occupancy level of that vehicle on that time
            """
            vehicle = request.args.get('vehicle')
            departureTime = request.args.get('departureTime')
            # Todo: return a matrix with (from, to) combinations

            d = {'prediction': 'unknown'}
            return jsonify(d)

        @self.app.route('/predict_by_from_to', methods=['GET'])
        def predict_by_from_to():
            """
            Predicts the occupancy level, given a certain departure and arrival station and time
            :return: the predicted occupancy level
            """
            departureTime = request.args.get('departureTime')
            _from = request.args.get('from')
            _to = request.args.get('to')
            # Todo: lookup the right vehicle and call predict()

            d = {'prediction': 'unknown'}
            return jsonify(d)

        @self.app.route('/predict', methods=['GET'])
        def predict():
            """
            Predicts the occupancy level, given a certain departure time, arrival and departure station and departure time
            :return: the predicted occupancy level
            """
            departureTime = parse(request.args.get('departureTime'))
            vehicle = request.args.get('vehicle')
            _from = request.args.get('from')
            _to = request.args.get('to')
            pred = self.predict(departureTime, vehicle, _from, _to)
            print(pred)
            d = {'prediction': ['low', 'medium', 'high'][int(np.argmax(pred))],
                 'low_probability': str(pred[0]), 'medium_probability': str(pred[1]),
                 'high_probability': str(pred[2])}
            return jsonify(d)


class LogParser(threading.Thread):
    """
    Thread that polls the iRail API to check for new occupancy logs, parses them and stores them
    """
    def __init__(self, mongoDAO):
        super(LogParser, self).__init__()
        self.mongoDAO = mongoDAO

    def onThread(self, function, *args, **kwargs):
        self.q.put((function, args, kwargs))

    def run(self):
        min_date = None
        while 1:
            try:
                min_date = parse_logs('https://api.irail.be/logs/', self.mongoDAO, min_date)
                print(min_date)
            except Exception:
                raise
            # Poll the API every 60 seconds
            time.sleep(60)


class ParamOpt(threading.Thread):
    """
    Thread that polls the iRail API to check for new occupancy logs, parses them and stores them
    """

    def __init__(self, xgb_clf):
        super(ParamOpt, self).__init__()
        self.xgb_clf = xgb_clf

    def onThread(self, function, *args, **kwargs):
        self.q.put((function, args, kwargs))

    def run(self):
        self.xgb_clf.optimize_hyperparams()


# Run this only if the script is ran directly.
if __name__ == '__main__':

    SERVER_IP = 'localhost'
    HOST_IP = 8000

    try:
        print('Starting the server...')
        mongoDAO = SpitsGidsMongoDAO('localhost', 9000)
        log_parser = LogParser(mongoDAO)
        log_parser.start()

        server = SpitsGidsServer(SERVER_IP, HOST_IP, mongoDAO)
        print('started')
    except Exception:
        print('Caught FATAL exception:')
        raise
