from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext(appName = "spark")
sqlContext = SQLContext(sc)

#Splitting and creating tables for precipitation and stations
prec = sc.textFile("BDA/input/precipitation-readings.csv")
parts = prec.map(lambda l: l.split(";"))
precReadings = parts.map(lambda p: Row(station=p[0], date=p[1], year=p[1].split("-")[0], month=p[1].split("-")[1], time=p[2], precipitation=float(p[3]), quality=p[4] ))

schemaPrecReadings = sqlContext.createDataFrame(precReadings) 
schemaPrecReadings.registerTempTable("percReadings")

stat = sc.textFile("BDA/input/stations-Ostergotland.csv")
parts = stat.map(lambda l: l.split(";"))
statReadings = parts.map(lambda p: Row(station=p[0]))

schemaStatReadings = sqlContext.createDataFrame(statReadings) 
schemaStatReadings.registerTempTable("statReadings")

# Sort and add precipitation for station at year and month
filtered_precReadings = schemaPrecReadings.filter( (schemaPrecReadings.year >= 1993) & (schemaPrecReadings.year <= 2016) )
# Join with station data to get only the stations in Östergotland
joined_data = filtered_precReadings.join(schemaStatReadings, "station")

# Calculate total monthly precipitation for each station
total_monthly_precipitation = joined_data.groupBy('year', 'month', 'station').agg(F.sum('precipitation').alias('total_monthly_precipitation'))

# Calculate the average monthly precipitation across all stations for each year and month
average_monthly_precipitation = total_monthly_precipitation.groupBy('year', 'month').agg(F.avg('total_monthly_precipitation').alias('avg_monthly_precipitation'))

#Descending order
average_monthly_precipitation = average_monthly_precipitation.orderBy('avg_monthly_precipitation', ascending=False)
average_monthly_precipitation.show()
