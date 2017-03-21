import logging
import datetime as dt
import pytz
from PyWeather.weather.units import *


class VantageMeasure(object):
    def __init__(self, archiveRecord):
        # see the PyWeather davis_weatherLinkIP module for details about fields
        filters = {
            'TempOut': self.set_FinC,
            'TempOutHi': self.set_FinC,
            'TempOutLow': self.set_FinC,
            'RainRate': self.set_rain_in_mm,
            'RainRateHi': self.set_rain_in_mm,
            'Barometer': self.set_minHg_in_Pa,
            'SolarRad': self.addParameter, # Average Solar Rad over the archive period. Units are (Watts / m 2 )
            'WindSamps': self.skipValue,
            'TempIn': self.set_FinC,
            'HumIn': self.set_in_percent,
            'HumOut': self.set_in_percent,
            'WindAvg': self.set_mph_in_kph,
            'WindHi': self.set_mph_in_kph,
            'WindHiDir': self.winDir_in_Deg,
            'WindAvgDir': self.winDir_in_Deg,
            'UV': self.addParameter,
            'ETHour': self.set_milliin_in_mm,
            'SolarRadHi': self.addParameter,
            'UVHi': self.addParameter,
            'ForecastRuleNo': self.skipValue,
            'LeafTemps': self.skipValue,
            'LeafWetness': self.skipValue,
            'SoilTemps': self.skipValue,
            'RecType': self.skipValue,
            'ExtraHum': self.skipValue,
            'ExtraTemps': self.skipValue,
            'SoilMoist': self.skipValue,
            # calculate values
            'HeatIndex': self.set_FinC,
            'Year': self.addParameter,
            'Day': self.addParameter,
            'Month': self.addParameter,
            'Hour': self.addParameter,
            'Min': self.addParameter,
            'DateStamp': self.skipValue,
            'DewPoint': self.set_FinC,
            'WindChill': self.set_FinC,
            'TimeStamp': self.skipValue,
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
    def set_FinC(self, measure, value):
        '''
        Add the measure in fahrenheit to the object as celsius value
        :param measure: name of the parameter to be added
        :param value: the temperature int fahrenheit
        :return: add parameter with name "measure" and value "value" in Celsius Degree to the object.
        '''
        self.addParameter(measure, fahrenheit_to_celsius(value))

    def set_minHg_in_Pa(self, measure, value):
        '''
        Add the measure in milliinches of Hg to the object as Pascal value
        :param measure: name of the parameter to be added
        :param value: the pressure in milli-inches of Hg
        :return: add parameter with name "measure" and value "value"*1000 converted in Pascals using the NIST
        conventional conversion to the object.
        '''
        self.addParameter(measure, incConv_to_Pa(value * 1000))

    def set_rain_in_mm(self, measure, value):
        '''
        Set the rainfall measure in mm
        :param measure: name of the parameter to be added
        :param value: number of clicks of the rain sensor in the archive time.
        :return: add parameter with name "measure" and value "value*0.2" to the object. 0.2 is the conversion rate of
        one click in mm for the vantagePro2 see Davis Spec sheet
        '''
        self.addParameter(measure, value*0.2)

    def set_in_percent(self, measure, value):
        '''
        Add the paremtere "measure" with value 0-255 to percentage.
        :param measure: name of the parameter to be added
        :param value: value in a 0-255 integer interval
        :return: add a parameter with name measure and value "value" to the object
        '''
        self.addParameter(measure, int(round(value*100.0/255)))

    def set_mph_in_kph(self, measure, value):
        '''
        Add the paremtere "measure" with value in mph to kph.
        :param measure: name of the parameter to be added
        :param value: value in mph
        :return: add a parameter with name measure and value "value" in kph to the object
        '''
        self.addParameter(measure, mph_to_km_hr(value))

    def winDir_in_Deg(self, measure, value):
        '''
        Add the wind direction constant converted into decimal degrees as parameter to the object
        :param measure: name of the parameter to be added
        :param value: value to be converted using the vantage wind direction protocol. See Davis serial protocol documentation.
        :return: add a parameter with name measure and value "value" in degrees to the object
        '''
        self.addParameter(measure, value*22.5)

    def set_milliin_in_mm(self, measure, value):
        # todo: docs
        v = (value/1000.0)*25.4
        self.addParameter(measure, v)