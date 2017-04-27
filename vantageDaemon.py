from vantageEntity import VantageMeasure
from PyWeather.weather.stations.davis_weatherLinkIP import *
import datetime as dt
from dateutil import parser as duParser
import pytz
from influxdb import InfluxDBClient
import time, socket
# <!------------------------------------------ LOGGING ---------------------------------------------->

import logging

## Compelte path to the log file.
logfile = "readings.log"

##
# Initialize the logging into the file defined by the logfile variable, set the verbose level of the logging and define
# the header string int he format datetime - debug level - message.
logging.basicConfig(
    filename=logfile,
    level=logging.WARNING,
    #level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# <!---------------------------------------- CONFIGURATION --------------------------------------------->
# Console IP
IP = "xxx.xxx.xxx.xxx"
# Davis default port
PORT = 22222
# Console logging interval in minutes Valid values are (1, 5, 10, 15, 30, 60, and 120). Results are undefined if you try to select an archive period not on the list.
ARCHIVE_INTERVAL = 1
DBHOST = 'localhost'
DBPORT = 8086
DBUID = 'root'
DBPSWD = 'root'
DBNAME = 'examplerow'
MEASURE_NAME = 'auto'
LOCAL_TZ = pytz.timezone('Europe/Rome')


def setConsoleTime(console):
    consoleTime = console.getTime()
    # get currente datetime truncated to milliseconds
    now = dt.datetime.now()
    print('console time: ' + consoleTime.isoformat())
    if consoleTime.isoformat() is not now.isoformat()[:-3]:
        logging.warning('time delta: ' + str(now - consoleTime) + ' fix time! '+now.isoformat()+' '+consoleTime.isoformat())
        # console.setTime(dt.datetime.now())
    logging.debug("archiveTime: " + str(console._archive_time))


if __name__ == '__main__':
    print("Connect to Console " + IP + " on port " + str(PORT))
    client = InfluxDBClient(DBHOST, DBPORT, DBUID, DBPSWD, DBNAME)
    # client.query("create database test")
    client.create_database(DBNAME)
    # load the last record
    # retrive the data from the console
    stationConnected = False
    while True:
        try:
            while not stationConnected:
                console = VantagePro(IP, PORT, ARCHIVE_INTERVAL)
                #setConsoleTime(console)
                logging.warning("console connected. Getting last records...")
                if MEASURE_NAME == 'auto' or MEASURE_NAME == 'Auto':
                    result = client.query('select * from Barometer order by time DESC limit 1')
                else:
                    result = client.query('select * from ' + MEASURE_NAME + ' order by time DESC limit 1')
                try:
                    # parse the last record and retrieve the ts
                    record = result.items()[0][1].next()['time']
                    # set the archive time to 10 min ago
                    # ts = dt.datetime.now() - dt.timedelta(minutes=5)
                    ts = duParser.parse(record)
                    # print ts.isoformat()
                    ts = ts.astimezone(LOCAL_TZ)  # from UTC in the database to localzone of the console
                    logging.warning("get records from: " + ts.isoformat() + str(ts.tzinfo))
                    console.setArchiveTime(ts)
                except IndexError as e:
                    # download all the records in console
                    logging.warning("downlaod all the points")
                    ts = None
                stationConnected = True
            console.parse()
            # save data to influxdb using the single measurement feature
            json = []
            for record in console.fields:
                entity = VantageMeasure(record, LOCAL_TZ)
                if MEASURE_NAME == 'auto' or MEASURE_NAME == 'Auto':
                    # save as multiple measurements, one for parameter
                    json.extend(entity.jsonify_by_row())
                else:
                    # save as a single measure with one field for parameter
                    json.append(entity.jsonify(MEASURE_NAME))
                logging.debug(str(json.__len__()) + " record added to JSON")
            client.write_points(json)
            # logging.warning(str(json.__len__()) + " data saved")
            sleepSec = 60 - dt.datetime.now().second
            logging.info("waiting the end of minute for " + str(sleepSec) + " sec")
            time.sleep(sleepSec)
            while (dt.datetime.now().minute % 5) != 0:
                sleepSec = 60 - dt.datetime.now().second
                logging.info("sleep for " + str(sleepSec) + " sec")
                time.sleep(sleepSec)
        except NoDeviceException as e:
            logging.error("Console connection lost waiting and retry... " + e.message)
            time.sleep(30)
            stationConnected = False
        except socket.error as e:
            logging.error("Network Connection lost " + e.message + ' retry')
            stationConnected = False
