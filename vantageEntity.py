import logging
import datetime as dt
import pytz
from PyWeather.weather.units import *


class VantageMeasure(object):
    def __init__(self, archiveRecord, timezone):
        '''
        create the influxdb measure entity.
        :param archiveRecord: the record received from the vantage
        :param timezone: the pytz timezone object
        '''
        # see the PyWeather davis_weatherLinkIP module for details about fields
        filters = {
            'TempOut': self._set_dF_in_C,
            'TempOutHi': self._set_FinC,
            'TempOutLow': self._set_FinC,
            'RainRate': self._set_rain_in_mm,
            'RainRateHi': self._set_rain_in_mm,
            'Barometer': self._set_minHg_in_Pa,
            'SolarRad': self._addParameter,  # Average Solar Rad over the archive period. Units are (Watts / m 2 )
            'WindSamps': self._skipValue,
            'TempIn': self._set_FinC,
            'HumIn': self._set_in_percent,
            'HumOut': self._set_in_percent,
            'WindAvg': self._set_mph_in_mps,
            'WindHi': self._set_mph_in_mps,
            'WindHiDir': self._winDir_in_Deg,
            'WindAvgDir': self._winDir_in_Deg,
            'UV': self._addParameter,
            'ETHour': self._set_milliin_in_mm,
            'SolarRadHi': self._addParameter,
            'UVHi': self._addParameter,
            'ForecastRuleNo': self._skipValue,
            'LeafTemps': self._skipValue,
            'LeafWetness': self._skipValue,
            'SoilTemps': self._skipValue,
            'RecType': self._skipValue,
            'ExtraHum': self._skipValue,
            'ExtraTemps': self._skipValue,
            'SoilMoist': self._skipValue,
            # calculate values
            'HeatIndex': self._set_FinC,
            'Year': self._addParameter,
            'Day': self._addParameter,
            'Month': self._addParameter,
            'Hour': self._addParameter,
            'Min': self._addParameter,
            'DateStamp': self._skipValue,
            'DewPoint': self._set_FinC,
            'WindChill': self._set_FinC,
            'TimeStamp': self._skipValue,
            'DateStampUtc': self._addParameter
        }
        # populate the object with all the parameters in archive record using the suitable filters
        for measure, value in archiveRecord.iteritems():
            filters[measure](measure, value)
        # compress Year, Month, Day, Minute into the time attribute as
        time = dt.datetime(self.Year, self.Month, self.Day, self.Hour, self.Min)
        self.time = timezone.localize(time)
        self.__delattr__("Year")
        self.__delattr__("Month")
        self.__delattr__("Day")
        self.__delattr__("Hour")
        self.__delattr__("Min")

    def _skipValue(self, measurement, value):
        '''
        This method implements a notch filter on the inserted parameters.
        :param measurement: the measurement name
        :param value: the measurement value
        '''
        logging.info("the measurement " + measurement + " has been skipped")

    def _addParameter(self, measure, value):
        self.__setattr__(measure, value)

    def jsonify(self, measurement="Vantage"):
        fields = {}
        json_body = {
            "measurement": measurement
        }
        for attribute, value in self.__dict__.iteritems():
            if attribute == "time":
                # add the unix timestamp to the influxdb json
                time = value.astimezone(pytz.timezone('UTC'))
                json_body["time"] = time.isoformat()
                print('record time = ', time.isoformat(), time.tzinfo)
            else:
                fields[attribute] = value
        json_body["fields"] = fields
        return json_body

    ############################ FILTER IMPLEMENTATION ###############################
    def _set_FinC(self, measure, value):
        '''
        Add the measure in fahrenheit to the object as celsius value
        :param measure: name of the parameter to be added
        :param value: the temperature int fahrenheit
        :return: add parameter with name "measure" and value "value" in Celsius Degree to the object.
        '''
        self._addParameter(measure, fahrenheit_to_celsius(value))

    def _set_dF_in_C(self, measure, value):
        '''
        Add the measure in fahrenheit/10 to the object as celsius value
        :param measure: name of the parameter to be added
        :param value: the temperature int fahrenheit
        :return: add parameter with name "measure" and value "value" in Celsius Degree to the object.
        '''
        self._addParameter(measure, fahrenheit_to_celsius(value / 10.0))

    def _set_minHg_in_Pa(self, measure, value):
        '''
        Add the measure in milliinches of Hg to the object as Pascal value
        :param measure: name of the parameter to be added
        :param value: the pressure in milli-inches of Hg
        :return: add parameter with name "measure" and value "value"*1000 converted in Pascals using the NIST
        conventional conversion to the object.
        '''
        self._addParameter(measure, incConv_to_Pa(value * 1000))

    def _set_rain_in_mm(self, measure, value):
        '''
        Set the rainfall measure in mm
        :param measure: name of the parameter to be added
        :param value: number of clicks of the rain sensor in the archive time.
        :return: add parameter with name "measure" and value "value*0.2" to the object. 0.2 is the conversion rate of
        one click in mm for the vantagePro2 see Davis Spec sheet
        '''
        self._addParameter(measure, value * 0.2)

    def _set_in_percent(self, measure, value):
        '''
        Add the paremtere "measure" with value 0-255 to percentage.
        :param measure: name of the parameter to be added
        :param value: value in a 0-255 integer interval
        :return: add a parameter with name measure and value "value" to the object
        '''
        self._addParameter(measure, int(round(value * 100.0 / 255)))

    def _set_mph_in_mps(self, measure, value):
        '''
        Add the paremtere "measure" with value in mph to m/s.
        :param measure: name of the parameter to be added
        :param value: value in mph
        :return: add a parameter with name measure and value "value" in m/s to the object
        '''
        self._addParameter(measure, mph_to_m_sec(value))

    def _winDir_in_Deg(self, measure, value):
        '''
        Add the wind direction constant converted into decimal degrees as parameter to the object
        :param measure: name of the parameter to be added
        :param value: value to be converted using the vantage wind direction protocol. See Davis serial protocol documentation.
        :return: add a parameter with name measure and value "value" in degrees to the object
        '''
        self._addParameter(measure, value * 22.5)

    def _set_milliin_in_mm(self, measure, value):
        '''
        Add amount of rain in mm as parameter to the object
        :param measure: name of the parameter to be added
        :param value: value in milli inches. See Davis serial protocol documentation.
        :return: add a parameter with name measure and value "value" in degrees to the object
        '''
        v = (value / 1000.0) * 25.4
        self._addParameter(measure, v)
