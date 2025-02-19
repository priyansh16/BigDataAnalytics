#Provide a list of stations with their associated maximum measured temperatures and maximum measured daily precipitation. 
#Show only those stations where the maximum temperature is between 25 and 30 degrees and maximum daily precipitation is between 100 mm and 200mm.
#In this exercise you will use the temperature-readings.csv and precipitation-readings.csv files.
#The output should contain the following information:
#Station number, maximum measured temperature, maximum daily precipitation


from pyspark import SparkContext

sc = SparkContext(appName = "spark")

temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
per_file = sc.textFile("BDA/input/precipitation-readings.csv")
lines_temperature = temperature_file.map(lambda line: line.split(";"))
lines_precipitation = per_file.map(lambda line: line.split(";"))

#maps the station to temperature (key, value) = (station, temperature)
stat_temperature = lines_temperature.map(lambda x: (x[0], float(x[3])))
#maps the station to percipitation (key, value) = ((station, date)), percipitation)
stat_precipitation = lines_precipitation.map(lambda x: ((x[0], x[1]), float(x[3])))

#this needs to be done since there are two values for each day
#sums up the daily precipitation
stat_precipitation = stat_precipitation.reduceByKey(lambda x1, x2: (x1 + x2))
stat_precipitation = stat_precipitation.map(lambda x: (x[0][0], x[1]))


#gets the max temprature for each station
max_temperature = stat_temperature.reduceByKey(max)
#filter values that are outside of the searched temperatures
searched_temperature = max_temperature.filter(lambda x: int(x[1])>=25 and int(x[1])<=30)

#max percipitation for each station and day
max_precipitation = stat_precipitation.reduceByKey(max)
#filter percipitation that are outside of the searced percipitation
searched_pecipitation = max_precipitation.filter(lambda x: int(x[1])<=200 and int(x[1])>=100)

#combines the two 
output=searched_pecipitation.join(searched_temperature)
output.saveAsTextFile("BDA/output")