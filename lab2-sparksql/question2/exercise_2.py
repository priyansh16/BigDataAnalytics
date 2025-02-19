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

#Filtering previous way
# year_temperature = schemaTempReadings.filter(lambda x: int(x[0])>=1950 and int(x[0])<=2014)

# Filtering using SQL-like expressions to take the values between 1950 and 2014 where temp is higher than 10 degrees
filtered_data = schemaTempReadings.filter((schemaTempReadings['year'] >= 1950) & (schemaTempReadings['year'] <= 2014) & (schemaTempReadings['temp'] > 10))


# Counting the number of readings for each month
monthly_counts = filtered_data.groupBy('year', 'month').agg(F.count('station').alias('count'))
monthly_counts = monthly_counts.orderBy('count', ascending=False)
monthly_counts.show()

# Taking only distinct readings from each station for each month
distinct_readings = filtered_data.select('station', 'year', 'month').distinct()

# Counting the number of distinct readings for each month
distinct_monthly_counts = distinct_readings.groupBy('year', 'month').agg(F.count('station').alias('distinctCount'))
distinct_monthly_counts = distinct_monthly_counts.orderBy('distinctCount', ascending=False)
distinct_monthly_counts.show()

"""
2.
year, month, value ORDER BY value DESC 
year, month, value ORDER BY value DESC
"""

