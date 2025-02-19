#!/usr/bin/env python3

from pyspark import SparkContext

sc = SparkContext(appName = "exercise 3")
# This path is to the file on hdfs
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))

# filter the temperature based on year 
filtered_temperature = lines.filter(lambda x: int(x[1][:4]) >= 1960 and int(x[1][:4]) <= 2014)

#Extracting year, month, station and temperature reading 
station_month_temp = filtered_temperature.map(lambda x:((x[1][0:4], x[1][5:7], x[0]), float(x[3])))

#count for each month
group_by_month_stations = station_month_temp.groupByKey()

# count from average temperature for each month each station
average_monthly_temp = group_by_month_stations.mapValues(lambda temps: round(sum(temps)/len(temps),2))

#print the output
average_monthly_temp.saveAsTextFile("BDA/output/average_temp")
