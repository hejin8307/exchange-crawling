

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == "__main__":
	spark = SparkSession.builder.appName("Exchange").getOrCreate()
		
	df1 = spark.read.load("hdfs:///user/maria_dev/project", format="csv", sep=",", inferSchema="true", header="true", encoding="UTF-8")
	df2 = df1.withColumn("year", split(col("date"), "\\.").getItem(0)).withColumn("month", split(col("date"), "\\.").getItem(1)).withColumn("day", split(col("date"), "\\.").getItem(2))		
	df2.createOrReplaceTempView("exchange")

	result = spark.sql("""
		SELECT country, year, avg(exchange) as average
		FROM exchange
		GROUP BY country, year
		HAVING year > '2004'
		ORDER BY country, year
		""")

	for row in result.collect():
		print(row.country, row.year, row.average)

