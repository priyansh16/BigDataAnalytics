from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext(appName = "spark")
sqlContext = SQLContext(sc)

rdd = sc.textFile("BDA/input/temperature-readings.csv")
parts = rdd.map(lambda l: l.split(";"))
tempReadings = parts.map(lambda p: Row(station=p[0], date=p[1], year=p[1].split("-")[0], month=p[1].split("-")[1], time=p[2], temp=float(p[3]), quality=p[4] ))

schemaTempReadings = sqlContext.createDataFrame(tempReadings) 
schemaTempReadings.registerTempTable("tempReadings")

filtered_data = schemaTempReadings.filter((schemaTempReadings['year'] >= 1960) & (schemaTempReadings['year'] <= 2014))

# Not fully sure if this will provide the average temperature
monthly_avg_temp = filtered_data.groupBy('year', 'month', 'station').agg(F.avg('temp').alias('avgMonthlyTemperature'))

# Order the result by average monthly temperature in descending order
monthly_avg_temp = monthly_avg_temp.orderBy('avgMonthlyTemperature', ascending=False)

monthly_avg_temp.show()
"""
Find the average monthly temperature for each available station in Sweden. Your result should include average temperature for each station for each month in the period of 1960- 2014. 
Bear in mind that not every station has the readings for each month in this timeframe. In this exercise you will use the temperature-readings.csv file.
The output should contain the following information:
3.
year, month, station, avgMonthlyTemperature ORDER BY avgMonthlyTemperature DESC
"""