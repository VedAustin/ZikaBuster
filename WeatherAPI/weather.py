#!/usr/bin/env python

import os  # used when Python needs to work with the OS/machine it's being processed on
import time  # used to time functions
import cred  # getting API creds
import json  # handling API requests
import requests  # Main Python module for making HTTP Requests
import datetime  # working with timestamps from API
import numpy  # for fancy stats
import pprint  # pretty printing
from datetime import timedelta  # more granular imports
from dateutil.parser import parse  # to validate date


class Weather(object):
    """Class object to get Weather Data."""

    def __init__(self, lat, lon, startdate, enddate=None, days=7, units='m'):
        """Parameter required to get weather data."""
        self.startdate = startdate
        self.enddate = enddate
        self.lat = lat
        self.lon = lon
        self.days = days
        self.units = units
        self.apikey = cred.cred['apikey']

        self._validate()

        self.params = {'Humidity': 'dewPt',
                       'Temperature': 'temp',
                       'Wind Speed': 'wspd',
                       'Precipitation': 'precip_total',
                       'Pressure': 'pressure'}

        # returns aggregated data for 7 days
        self.result = {}

    def _validate_date(self, date_entry):
        try:
            parse(date_entry)
        except ValueError:
            return False

        return True

    def _validate(self):
        if not self._validate_date(self.startdate):
            raise ValueError('Invalid Date Format')

        if self.enddate:
            if not self._validate_date(self.enddate):
                raise ValueError('Invalid Date Format')
        else:
            enddate = parse(self.startdate) - timedelta(days=self.days)
            self.enddate = str(enddate.month) + '/' + str(enddate.day) + \
                '/' + str(enddate.year)

        self.startdate = self._format_date(self.startdate)
        self.enddate = self._format_date(self.enddate)

        # Fliping Start Date and End Date as Start Date < End Date
        self.startdate, self.enddate = self.enddate, self.startdate

    def _format_date(self, date_input):
        date = parse(date_input)

        year = str(date.year)

        if date.month < 10:
            month = str('0') + str(date.month)
        else:
            month = str(date.month)

        if date.day < 10:
            day = str('0') + str(date.day)
        else:
            day = str(date.day)

        date = year + month + day
        return date

    def _build_request_url(self):
        url = "http://api.weather.com/v1/geocode/" + str(self.lat) + "/" + \
            str(self.lon) + "/observations/historical.json?apiKey=" + \
            self.apikey + "&units=" + self.units + \
            "&startDate=" + str(self.startdate) + \
            "&endDate=" + str(self.enddate)

        return url

    def _get_weather_data(self):
        url = self._build_request_url()
        response = requests.get(url).json()
        return response['observations']

    def _get_stats(self, response, param):

        param_stat = {
            'min': None,
            'max': None,
            'mean': None
        }

        param_list = []

        for entry in response:
            if entry[param]:
                param_list.append(entry[param])

        try:
            param_stat['min'] = min(param_list)
            if param_stat['min'] is None:
                param_stat['min'] = 0
        except ValueError as val_err:
            if val_err == 'min() arg is an empty sequence':
                param_stat['min'] = 0

        try:
            param_stat['max'] = max(param_list)
            if param_stat['max'] is None:
                param_stat['max'] = 0
        except ValueError as val_err:
            if val_err == 'max() arg is an empty sequence':
                param_stat['max'] = 0

        if param_stat['min'] == 0 and param_stat['max'] == 0:
            param_stat['mean'] = 0
        else:
            try:
                param_stat['mean'] = numpy.mean(param_list)
            except ValueError as val_err:
                if val_err == 'mean() arg is an empty sequence':
                    param_stat['mean'] = 0

        return param_stat

    def _generate_response_content(self, response):
        for item in self.params:
            param = self.params[item]
            param_stats = self._get_stats(response, param)

            for stat in param_stats:
                param_key = str(param) + '_' + str(stat)
                self.result[param_key] = param_stats[stat]

    def get_data(self):
        """Class method to get weather data."""
        response = self._get_weather_data()
        self._generate_response_content(response)
        return self.result

    def print_data(self):
        """Class method to print data."""
        pprint.pprint(self.result)
