import time
from flask import Flask, request, jsonify
from flask_restful import Api
import threading

from datascraper import parse_logs
from mongoDAO import SpitsGidsMongoDAO

from xgboost. import load_model

SERVER_IP = 'localhost'
HOST_IP = 8000
app = Flask(__name__)
api = Api(app)
xgb = None

# Start the restful server.
def start_restfulserver(local=False):
    # TODO: load trained xgb model from DB
    xgb = load_model
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
    departureTime = request.args.get('departureTime')
    vehicle = request.args.get('vehicle')
    _from = request.args.get('from')
    _to = request.args.get('to')

    d = {'prediction': 'unknown'}
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
        mongoDAO = SpitsGidsMongoDAO('localhost', 9000)
        log_parser = LogParser()
        log_parser.start()
    except Exception:
        print('Caught FATAL exception:')
        raise
