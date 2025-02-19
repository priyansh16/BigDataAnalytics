from pyspark import SparkContext

sc = SparkContext(appName = "spark")

percipitation_file = sc.textFile("BDA/input/precipitation-readings.csv")
station_file = sc.textFile("BDA/input/stations-Ostergotland.csv")
lines_precipitation = percipitation_file.map(lambda line: line.split(";"))
# Retrieves the stations in Östergötland
stations = station_file.map(lambda line: line.split(";")[0])
stations = stations.collect()

#Maps the following (key, value) = ((station, year-month),temperature)
stat_precipitation = lines_precipitation.map(lambda x: ((x[0], x[1][:7]), (float(x[3]))))
ostgot_precipation = stat_precipitation.filter(lambda x: x[0][0] in stations)
ostgot_precipation = ostgot_precipation.filter(lambda x: int(x[0][1][0:4])>=1993 and int(x[0][1][0:4])<=2016)
ostgot_monthly_precipation = ostgot_precipation.reduceByKey(lambda x1, x2: (x1+x2))
#At this point we have added together the total precipitation for each station for each month every year

# Add a 1 to each value so we are able to divide by the amount of months we have for each station
ostgot_monthly_precipation = ostgot_monthly_precipation.map(lambda x: (x[0][1], (x[1], 1)))
# Sums together both the count and precipitation of all the keys that are the same
ostgot_monthly_total = ostgot_monthly_precipation.reduceByKey(lambda x1, x2:(x1[0]+x2[0], x1[1]+x2[1]))
# Gets monthly average per station by dividing 
ostgot_monthly_average = ostgot_monthly_total.mapValues(lambda x: x[0] / x[1])

ostgot_monthly_average.saveAsTextFile("BDA/output")

