from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == "__main__":
        spark = SparkSession.builder.appName("Gap").getOrCreate()

        df1 = spark.read.load("hdfs:///user/maria_dev/project", format="csv", sep=",", inferSchema="true", header="true", encoding="UTF-8")
        df2 = df1.withColumn("year", split(col("date"), "\\.").getItem(0)).withColumn("month", split(col("date"), "\\.").getItem(1)).withColumn("day", split(col("date"), "\\.").getItem(2))
        df2.createOrReplaceTempView("gap")

        result = spark.sql("""
                select country, year, avg(buying_cash - selling_cash) as gap
		from gap
		where year > '2004'
		group by country, year 
		order by country, year
		""")

	for row in result.collect():
		print(row.country, row.year, row.gap)

