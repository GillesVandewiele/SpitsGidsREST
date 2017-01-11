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

SERVER_IP = 'localhost'
HOST_IP = 8000
app = Flask(__name__)
api = Api(app)


def load_xgb_clf():
    feature_vectors = []
    for feature_vector in mongoDAO.db['features'].find({}):
        feature_vectors.append(feature_vector)

    label_col = 'occupancy'
    df = pd.DataFrame(feature_vectors)
    # df = pd.get_dummies(df, columns=['week_day', 'vehicle_type', 'vehicle_id'])
    df = df.drop(['_id', 'log_id'], axis=1)
    global feature_cols
    feature_cols = list(set(df.columns) - {'occupancy'})
    return XGBModel(df, feature_cols, label_col)


def train_model():
    mongoDAO.process_unprocessed_logs()
    xgb_clf = load_xgb_clf()
    # if xgb_clf.parameters == {}:
    #     xgb_clf.optimize_hyperparams()
    xgb_clf.construct_model()
    return xgb_clf.model


# Start the restful server.
def start_restfulserver(local=False):
    global xgb
    xgb = train_model()
    if not local:
        app.run(host=SERVER_IP, port=HOST_IP)
    else:
        app.run(debug=True, port=HOST_IP)


@app.route('/predict_by_vehicle', methods=['GET'])
def predict_by_vehicle():
    """
    Predicts the occupancy level, given a certain vehicle identifier and its departure time
    :return: the predicted occupancy level of that vehicle on that time
    """
    vehicle = request.args.get('vehicle')
    departureTime = request.args.get('departureTime')

    d = {'prediction': 'unknown'}
    return jsonify(d)


@app.route('/predict_by_from_to', methods=['GET'])
def predict_by_from_to():
    """
    Predicts the occupancy level, given a certain departure and arrival station and time
    :return: the predicted occupancy level
    """
    departureTime = request.args.get('departureTime')
    _from = request.args.get('from')
    _to = request.args.get('to')

    d = {'prediction': 'unknown'}
    return jsonify(d)


@app.route('/predict', methods=['GET'])
def predict():
    """
    Predicts the occupancy level, given a certain departure time, arrival and departure station and departure time
    :return: the predicted occupancy level
    """
    departureTime = parse(request.args.get('departureTime'))
    vehicle = request.args.get('vehicle')
    _from = request.args.get('from')
    _to = request.args.get('to')
    df = pd.DataFrame([extract_features_prediction(departureTime, vehicle, _from, _to, mongoDAO)])
    df = df[feature_cols]
    print(df)
    pred = xgb.predict_proba(df)[0]

    d = {'prediction': ['low', 'medium', 'high'][int(np.argmax(pred))],
         'low_probability': str(pred[0]), 'medium_probability': str(pred[1]),
         'high_probability': str(pred[2])}
    return jsonify(d)


class LogParser(threading.Thread):
    def __init__(self):
        super(LogParser, self).__init__()

    def onThread(self, function, *args, **kwargs):
        self.q.put((function, args, kwargs))

    def run(self):
        min_date = None
        while 1:
            try:
                min_date = parse_logs('https://api.irail.be/logs/', mongoDAO, min_date)
                print(min_date)
            except Exception:
                raise
            time.sleep(2)


# Run this only if the script is ran directly.
if __name__ == '__main__':
    try:
        print('Starting the server...')
        global mongoDAO
        mongoDAO = SpitsGidsMongoDAO('localhost', 9000)
        log_parser = LogParser()
        log_parser.start()

        start_restfulserver()
    except Exception:
        print('Caught FATAL exception:')
        raise
