from pyspark.sql import SparkSession
from pyspark.sql.functions import unix_timestamp, from_unixtime

if __name__ == "__main__":
        spark = SparkSession.builder.appName("Buying").getOrCreate()

        df = spark.read.load("hdfs:///user/maria_dev/project", format="csv", sep=",", inferSchema="true", header="true", encoding="UTF-8")

        df.createOrReplaceTempView("buying")

        result = spark.sql("""
                SELECT country, from_unixtime(unix_timestamp(date, 'yyyy.MM.dd'), 'yyyy') as year, avg(buying_cash) as average
                FROM buying
                GROUP BY country, year
                HAVING year > '2004'
                ORDER BY country, year
                """)

        for row in result.collect():
                print(row.country, row.year, row.average)

