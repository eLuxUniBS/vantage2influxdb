import vantageEntity
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
    # level=logging.DEBUG,
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
DBNAME = 'example'
MEASURE_NAME = 'Vantage'
LOCAL_TZ = 'Europe/Rome'

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
                print "console connected. Getting last records..."
                result = client.query('select * from ' + MEASURE_NAME + ' order by time DESC limit 1')
                try:
                    # parse the last record and retrieve the ts
                    record = result.items()[0][1].next()['time']
                    # set the archive time to 10 min ago
                    # ts = dt.datetime.now() - dt.timedelta(minutes=5)
                    ts = duParser.parse(record)
                    logging.debug("archived last ts=" + ts.isoformat() + str(ts.tzinfo))
                    ts = ts.astimezone(pytz.timezone(LOCAL_TZ))
                    logging.debug(ts.isoformat() + str(ts.tzinfo))
                    console.setArchiveTime(ts)
                    logging.warning(
                        "Get records from " + str(ts.year) + "/" + str(ts.month) + "/" + str(ts.day) + " " + str(
                            ts.hour) + ":" + str(ts.minute))
                except IndexError as e:
                    # download all the records in console
                    ts = None
                stationConnected = True
            # get currente datetime truncated to milliseconds
            # now = dt.datetime.now()
            # consoleTime = console.getTime()
            # print('console time: ' + consoleTime.isoformat())
            # if consoleTime.isoformat() is not now.isoformat()[:-3]:
            #     logging.warning('time delta: '+str(now-consoleTime) + ' fix time!')
            #     console.setTime(dt.datetime.now())
            # logging.debug("archiveTime: " + str(console._archive_time))
            console.parse()
            # logging.warning(console.fields.__len__())
            # save data to influxdb using
            json = []
            for record in console.fields:
                entity = vantageEntity.VantageMeasure(record)
                json.append(entity.jsonify('Europe/Rome', MEASURE_NAME))
                logging.debug(str(json.__len__()) + " record added to JSON")
            client.write_points(json)
            logging.warning(str(json.__len__()) + " data saved")
            sleepSec = 60 - dt.datetime.now().second
            logging.debug("waiting the end of minute for " + str(sleepSec) + " sec")
            time.sleep(sleepSec)
            while (dt.datetime.now().minute % 5) != 0:
                sleepSec = 60 - dt.datetime.now().second
                logging.debug("sleep for " + str(sleepSec) + " sec")
                time.sleep(sleepSec)
        except NoDeviceException as e:
            logging.error("Console connection lost waiting a minute and retry... " + e.message)
            time.sleep(60)
            stationConnected = False
        except socket.error as e:
            logging.error("Connection lost " + e.message)
            stationConnected = False
