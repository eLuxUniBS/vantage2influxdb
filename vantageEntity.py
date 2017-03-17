import logging
import datetime as dt
import pytz
from PyWeather.weather.units import *

class VantageMeasure(object):
    def __init__(self, archiveRecord):
        filters = {
            'HumOut': self.addParameter,
            'HeatIndex': self.addParameter,
            'TempIn': self.addParameter,
            'Year': self.addParameter,
            'Day': self.addParameter,
            'Month': self.addParameter,
            'Hour': self.addParameter,
            'Min': self.addParameter,
            'HumIn': self.addParameter,
            'ExtraHum': self.skipValue,
            'DateStamp': self.skipValue,
            'DewPoint': self.addParameter,
            'RecType': self.addParameter,
            'WindChill': self.addParameter,
            'Pressure': self.addParameter,
            'WindAvgDir': self.addParameter,
            'SoilMoist': self.skipValue,
            'SoilTemps': self.skipValue,
            'WindSamps': self.addParameter,
            'WindAvg': self.addParameter,
            'UVHi': self.addParameter,
            'LeafTemps': self.skipValue,
            'WindHi': self.addParameter,
            'ETHour': self.addParameter,
            'ForecastRuleNo': self.skipValue,
            'TimeStamp': self.skipValue,
            'SolarRadHi': self.addParameter,
            'WindHiDir': self.addParameter,
            'TempOutLow': self.addParameter,
            'RainRateHi': self.addParameter,
            'LeafWetness': self.skipValue,
            'UV': self.addParameter,
            'SolarRad': self.addParameter,
            'ExtraTemps': self.skipValue,
            'TempOutHi': self.addParameter,
            'TempOut': self.addParameter,
            'RainRate': self.addParameter,
            'DateStampUtc': self.addParameter
        }
        # populate the object with all the parameters in archive record using the suitable filters
        for measure, value in archiveRecord.iteritems():
            filters[measure](measure, value)
        # compress Year, Month, Day, Minute into the time attribute
        self.time = dt.datetime(self.Year, self.Month, self.Day, self.Hour, self.Min)
        self.__delattr__("Year")
        self.__delattr__("Month")
        self.__delattr__("Day")
        self.__delattr__("Hour")
        self.__delattr__("Min")

    def skipValue(self, measurement, value):
        '''
        This method implements a notch filter on the inserted parameters.
        :param measurement: the measurement name
        :param value: the measurement value
        '''
        logging.info("the measurement " + measurement + " has been skipped")

    def addParameter(self, measure, value):
        self.__setattr__(measure, value)

    def jsonify(self, sourceTZ, measurement="Vantage"):
        fields = {}
        json_body = {
            "measurement": measurement
        }
        for attribute, value in self.__dict__.iteritems():
            if attribute == "time":
                # add the unix timestamp to the influxdb json todo: fix the timezone
                time = value.replace(tzinfo=pytz.timezone(sourceTZ)).astimezone(pytz.timezone('UTC'))
                json_body["time"] = time.isoformat()
                print('record time = ', time.isoformat(), time.tzinfo)
            else:
                fields[attribute] = value
        json_body["fields"] = fields
        return json_body

############################ FILTER IMPLEMENTATION ###############################
    def setFinC(self, measure, value):
        '''
        Add the measure in fahrenheit to the object as celsius value
        :param measure: the name of the measure
        :param value: the temperature int fahrenheit
        '''
        self.addParameter(measure, fahrenheit_to_celsius(value))