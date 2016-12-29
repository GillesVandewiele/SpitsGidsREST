import json
import requests
from dateutil.parser import parse


def parse_logs(url, mongoDAO, min_date=None):
    for line in requests.get(url).json():
        result, parsed_log = parse_log_line(line, min_date)
        if result == 2:
            print('break loop')
            break
        elif result == 1:
            if not mongoDAO.log_exists(parsed_log['vehicle_id'], parsed_log['querytime']):
                mongoDAO.insert_log(parsed_log)
                min_date = parsed_log['querytime']
    return min_date


def insert_logs_from_file(file, mongoDAO):
    succeeded_logs = 0
    faulty_logs = 0
    with open(file) as data_file:
        for line in data_file:
            occ_logline = json.loads(line)
            result, parsed_log = parse_log_line(occ_logline)
            if result == 1:
                if not mongoDAO.log_exists(parsed_log['vehicle_id'], parsed_log['querytime']):
                    mongoDAO.insert_log(parsed_log)
                    succeeded_logs += 1
            else: faulty_logs += 1
        return succeeded_logs, faulty_logs


def parse_log_line(logline, min_date=None):
    if logline['querytype'] == 'occupancy' and 'error' not in logline and 'querytime' in logline:
        print 'Got occupancy log'
        try:
            querytime = parse(logline['querytime'])
            from_id = logline['post']['from'].split('/')[-1]
            to_id = logline['post']['to'].split('/')[-1]
            occupancy = logline['post']['occupancy'].split('/')[-1]
            connection = logline['post']['connection']
            vehicle_id = connection.split('/')[-1]
            user_agent = logline['user_agent']

            if min_date is not None and min_date >= querytime:
                return 2, None

            if from_id[:2] == '00' and to_id[:2] == '00' and vehicle_id != 'undefined' and vehicle_id != '(null)' \
                    and len(to_id) > 2 and len(from_id) > 2:
                parsed_log = {}
                parsed_log['querytime'] = querytime
                parsed_log['from_id'] = from_id
                parsed_log['to_id'] = to_id
                parsed_log['vehicle_id'] = vehicle_id
                parsed_log['occupancy'] = occupancy
                parsed_log['connection'] = connection
                parsed_log['user_agent'] = user_agent
                parsed_log['processed'] = False
                print 'success'
                return 1, parsed_log
        except Exception as e:
            return 0, None
    return 0, None

from mongoDAO import SpitsGidsMongoDAO

mongoDAO = SpitsGidsMongoDAO('localhost', 9000)
#print(insert_logs_from_file('occupancy-until-20161029.newlinedelimitedjsonobjects', mongoDAO))
# print(insert_logs_from_file('data/occupancy-until-20161219.nldjson', mongoDAO))
mongoDAO.process_unprocessed_logs()
mongoDAO.load_stations_table('data/station_drukte_link.csv')
