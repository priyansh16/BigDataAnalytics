#!/usr/bin/env python3

from pyspark import SparkContext

sc = SparkContext(appName = "exercise 2")
# This path is to the file on hdfs
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))

# filter the temperature based on year and >10degree
filtered_temperature = lines.filter(lambda x: int(x[1][:4]) >= 1950 and int(x[1][:4]) <= 2014
                                    and float(x[3]) >10)

#count all reading for each months (yearmonth, 1) (key, value)
month_count = filtered_temperature.map(lambda x:((x[1][0:4],x[1][5:7]),1))

#count for each month
month_count =  month_count.reduceByKey(lambda a, b: a + b)

# count from distinct stations
distinct_monthly_count = filtered_temperature.map(lambda x: ((x[1][0:4], x[1][5:7], x[0]), 1))
distinct_monthly_count = distinct_monthly_count.distinct()
distinct_monthly_count = distinct_monthly_count.map(lambda x: ((x[0][0], x[0][1]), 1))
distinct_monthly_count = distinct_monthly_count.reduceByKey(lambda a, b: a + b)

#print the output
month_count.saveAsTextFile("BDA/output/monthly_count")
distinct_monthly_count.saveAsTextFile("BDA/output/distinct_monthly_count")

