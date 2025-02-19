from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext(appName="exercise 4")

sqlContext = SQLContext(sc)

#load rain data 
precipitation_file = sc.textFile("BDA/input/precipitation-readings.csv")
precipitation_lines = precipitation_file.map(lambda line: line.split(";"))
precipitationReadings = precipitation_lines.map(lambda p: Row(station=p[0], year=p[1].split("-")[0], month=p[1].split("-")[1], time=p[2], date=p[1], precipitation=float(p[3]), quality=p[4]))
precipitation_schema = sqlContext.createDataFrame(precipitationReadings)
precipitation_schema.registerTempTable("precipitation_readings")

# Load temperature data
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
temperature_lines = temperature_file.map(lambda line: line.split(";"))
temperature_readings = temperature_lines.map(lambda p: Row(station=p[0], year=p[1].split("-")[0], month=p[1].split("-")[1], time=p[2], temp=float(p[3]), quality=p[4]))
temperature_schema = sqlContext.createDataFrame(temperature_readings)
temperature_schema.registerTempTable("temperature_readings")

#Max temperature in internal based on each station
schema_temp_max = temperature_schema.groupBy('station').agg(F.max('temp').alias('temp'))
filtered_temperature = schema_temp_max.filter((schema_temp_max.temp >= 25) & (schema_temp_max.temp <= 30))
filtered_temperature = filtered_temperature.join(temperature_schema, ['station','temp'], 'inner').select('year', 'station', 'temp')

# Filter precipitation readings between 100mm and 200mm
# Get daily observations of rain and then filtering
schemaPrecipitationDaily = precipitation_schema.groupBy('date').sum('precipitation')
schemaPrecipitationDaily.show()
schemaPrecipitationDaily = schemaPrecipitationDaily.join(precipitation_schema, ['date'], 'inner')
schemaPrecipitationMax = schemaPrecipitationDaily.groupBy('station').agg(F.max('sum(precipitation)').alias('sumprecipitation'))
filtered_precipitation = schemaPrecipitationMax.filter((schemaPrecipitationMax.sumprecipitation >= 100) & (schemaPrecipitationMax.sumprecipitation <= 200))

# JOIN TEMP AND RAIN
schema_tot = filtered_precipitation.join(filtered_temperature, ['station'], 'inner').select('station', 'temp', 'sumprecipitation').orderBy('station', acsending=False).show()
# Display the result
schema_tot.saveAsTextFile("BDA/output")
 