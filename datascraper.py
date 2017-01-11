import json
import requests
from dateutil.parser import parse


def parse_logs(url, mongoDAO, min_date=None):
    """
    Parses all logs that are produced after min_date and stores them in the DB
    :param url: the url where the logs can be retrieved
    :param mongoDAO: DAO of the database to store the newly processed logs in
    :param min_date: only logs that are produced later than min_date are processed
    :return: the new min_date
    """
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
    """
    Parses all logs in a given file
    :param file: path of the file containing the logs
    :param mongoDAO: DAO of the database to store the newly processed logs in
    :return: the number of succeeded and failed logs
    """
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
    """
    Retrieve all fields from a log line, do some processing and return them as a dict
    :param logline: the log to process
    :param min_date: the log will only be processed if querytime < min_date
    :return: a dict containing all retrieved fields, which can be stored immediately as JSON in the DB.
    """
    if logline['querytype'] == 'occupancy' and 'error' not in logline and 'querytime' in logline:
        print('Got occupancy log')
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
                print('success')
                return 1, parsed_log
        except Exception as e:
            return 0, None
    return 0, None




from mongoDAO import SpitsGidsMongoDAO
#
mongoDAO = SpitsGidsMongoDAO('localhost', 9000)
# mongoDAO.clean_logs_table()
# mongoDAO.clean_features_table()
# # print(insert_logs_from_file('occupancy-until-20161029.newlinedelimitedjsonobjects', mongoDAO))
# # print(insert_logs_from_file('data/occupancy-until-20161219.nldjson', mongoDAO))
# print(insert_logs_from_file('data/limited_logs.nldjson', mongoDAO))
# mongoDAO.process_unprocessed_logs()
# mongoDAO.train_model()
# mongoDAO.optimize_hyperparams()
# mongoDAO.load_stations_table('data/stations.csv')
# print(mongoDAO.get_station_info_by_id('008844008'))
# mongoDAO.count_records_per_table()

mongoDAO.clean_logs_table()
mongoDAO.clean_features_table()
insert_logs_from_file('data/occupancy-until-20161219.nldjson', mongoDAO)
mongoDAO.process_unprocessed_logs()
