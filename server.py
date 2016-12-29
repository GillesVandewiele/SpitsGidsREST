import thread

import time
from flask import Flask, request, abort, jsonify
from flask_restful import Resource, Api
import json

from datascraper import parse_logs
from mongoDAO import SpitsGidsMongoDAO

SERVER_IP = 'localhost'
HOST_IP = 8000
app = Flask(__name__)
api = Api(app)


# Start the restful server.
def start_restfulserver(local=False):
    if not local:
        app.run(host=SERVER_IP, port=HOST_IP)
    else:
        app.run(debug=True, port=HOST_IP)


@app.route('/', methods=['GET'])
def make_prediction():
    # request.args.get('data')
    d = {'prediction': 'low'}
    return jsonify(d)


# Run this only if the script is ran directly.
if __name__ == '__main__':
    try:
        print 'Starting the server...'
        start_restfulserver()
        print 'Started the server...'
        min_date = None
        mongoDAO = SpitsGidsMongoDAO('localhost', 9000)
        while 1:
            try:
                min_date = parse_logs('https://api.irail.be/logs/', mongoDAO, min_date)
                print(min_date)
            except Exception:
                raise
            time.sleep(10)
    except Exception, err:
        raise
        print 'Caught FATAL exception: %s' % str(err)