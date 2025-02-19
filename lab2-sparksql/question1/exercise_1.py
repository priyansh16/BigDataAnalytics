from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext(appName = "spark")
sqlContext = SQLContext(sc)

rdd = sc.textFile("BDA/input/temperature-readings.csv")
parts = rdd.map(lambda l: l.split(";"))
tempReadings = parts.map(lambda p: Row(station=p[0], date=p[1], year=p[1].split("-")[0], time=p[2], temp=float(p[3]), quality=p[4] ))

schemaTempReadings = sqlContext.createDataFrame(tempReadings) 
schemaTempReadings.registerTempTable("tempReadings")

#Filtering previous way
# year_temperature = schemaTempReadings.filter(lambda x: int(x[0])>=1950 and int(x[0])<=2014)

# Filtering using SQL-like expressions
year_temperature = schemaTempReadings.filter((schemaTempReadings['year'] >= 1950) & (schemaTempReadings['year'] <= 2014))

# Get Max Temperature
# Groups the values by the same year then aggregates the values and takes the max. Renames the aggregated column to max_temp
df_max_temp = year_temperature.groupBy('year').agg(F.max('temp').alias('max_temp'))
max_temp_data = df_max_temp.orderBy('max_temp', ascending=False)
max_temp_data.show()

# Get Min Temperature
# Same logic as with max temperatures
df_min_temp = year_temperature.groupBy('year').agg(F.min('temp').alias('min_temp'))
min_temp_data = df_min_temp.orderBy('min_temp', ascending=False)
min_temp_data.show()

# Save DataFrames to text files
max_temp_data.write.csv("BDA/output/maxtemp")
min_temp_data.write.csv("BDA/output/mintemp")

"""
1.
year, station with the max, maxValue ORDER BY maxValue DESC 
year, station with the min, minValue ORDER BY minValue DESC
"""