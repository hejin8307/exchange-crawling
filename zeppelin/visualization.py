val df1 = spark.read.option("header", "true").csv("hdfs:///user/maria_dev/project")

val df2 = df1.withColumn("year", split(col("date"), "\\.").getItem(0)).withColumn("month", split(col("date"), "\\.").getItem(1)).withColumn("day", split(col("date"), "\\.").getItem(2))

val df3 = df2.selectExpr("country", "date", "cast(exchange as double)", "cast(buying_cash as double)", "cast(selling_cash as double)", "cast(sending_money as double)", "cast(getting_money as double)", "buying_TC", "selling_check", "cast(year as int)", "cast(month as int)", "cast(day as int)")
df3.createOrReplaceTempView("exchange")

df3.printSchema()

%sql
select country, year, avg(exchange) as average
from exchange
group by country, year
having year > '2004'
order by country, year

%sql
SELECT country, quarter(from_unixtime(unix_timestamp(date, 'yyyy.MM.dd'), 'yyyy-MM-dd')) AS quarter, avg(exchange) as average      
FROM exchange
where year > '2004'
group by country, quarter
order by country, quarter

%sql
select country, year, avg(buying_cash - selling_cash) as gap
from exchange
where year > '2004'
group by country, year
order by country, year