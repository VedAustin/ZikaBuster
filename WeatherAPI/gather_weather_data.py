#!/usr/bin/env python

import csv
import multiprocessing
from multiprocessing import Pool
from weather import Weather


def get_data(lat, lon, startdate):
    weather = Weather(lat=lat, lon=lon, startdate=startdate)
    try:
        response = weather.get_data()
        return response
    except KeyError:
        response = ['0'] * 15
        return response


def read_input_data():
    input_list = []

    with open('zika.csv', 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for item in reader:
            data = [item['\ufeffreport_date'],
                    item['location'],
                    item['lat'],
                    item['long']]
            input_list.append(data)

    return input_list


def write_output_data(output_list):
    csvfile = open('zika_weather.csv', "w")
    writer = csv.writer(csvfile, delimiter=',',
                        quotechar='"', quoting=csv.QUOTE_ALL)

    titles = ['report_date',
              'location',
              'lat',
              'long',
              'dewPt_max',
              'dewPt_mean',
              'dewPt_min',
              'precip_total_max',
              'precip_total_mean',
              'precip_total_min',
              'pressure_max',
              'pressure_mean',
              'pressure_min',
              'temp_max',
              'temp_mean',
              'temp_min',
              'wspd_max',
              'wspd_mean',
              'wspd_min']

    writer.writerow(titles)

    for row in output_list:
        writer.writerow(row)

    csvfile.close()


def map_data(item):
    output_list = []

    params = ['dewPt_max',
              'dewPt_mean',
              'dewPt_min',
              'precip_total_max',
              'precip_total_mean',
              'precip_total_min',
              'pressure_max',
              'pressure_mean',
              'pressure_min',
              'temp_max',
              'temp_mean',
              'temp_min',
              'wspd_max',
              'wspd_mean',
              'wspd_min']

    response = get_data(item[2],
                        item[3],
                        item[0])
    result = []
    result += [item[0], item[1], item[2], item[3]]

    for param in params:
        try:
            result.append(response[param])
        except:
            result.append('0')

    output_list.append(result)

    return output_list

if __name__ == '__main__':
    input_data = read_input_data()

    pool = Pool(32)
    output_list = pool.map(map_data, input_data)
    pool.close()
    pool.join()

    write_output_data(output_list)
