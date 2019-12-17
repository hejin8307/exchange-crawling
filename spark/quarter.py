

from pyspark.sql import SparkSession
from pyspark.sql.functions import unix_timestamp, from_unixtime
from pyspark.sql.functions import *

if __name__ == "__main__":
        spark = SparkSession.builder.appName("Quarter").getOrCreate()

        df1 = spark.read.load("hdfs:///user/maria_dev/project", format="csv", sep=",", inferSchema="true", header="true", encoding="UTF-8")
	df2 = df1.withColumn("year", split(col("date"), "\\.").getItem(0)).withColumn("month", split(col("date"), "\\.").getItem(1)).withColumn("day", split(col("date"), "\\.").getItem(2))
        df2.createOrReplaceTempView("quarter")

        result = spark.sql("""
                SELECT country, quarter(from_unixtime(unix_timestamp(date, 'yyyy.MM.dd'), 'yyyy-MM-dd')) as quarter, avg(exchange) as average
                FROM quarter
		WHERE year > '2004'
                GROUP BY country, quarter
                ORDER BY country, quarter
                """)

        for row in result.collect():
                print(row.country, row.quarter, row.average)

