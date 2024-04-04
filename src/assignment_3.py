from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Initialize SparkSession
spark = SparkSession.builder.appName("User Activity Analysis").getOrCreate()

#1.Create DataFrame with custom schema
schema = StructType([
    StructField("log_id", IntegerType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("action", StringType(), True),
    StructField("timestamp", StringType(), True)
])

data = [
    (1, 101, 'login', '2023-09-05 08:30:00'),
    (2, 102, 'click', '2023-09-06 12:45:00'),
    (3, 101, 'click', '2023-09-07 14:15:00'),
    (4, 103, 'login', '2023-09-08 09:00:00'),
    (5, 102, 'logout', '2023-09-09 17:30:00'),
    (6, 101, 'click', '2023-09-10 11:20:00'),
    (7, 103, 'click', '2023-09-11 10:15:00'),
    (8, 102, 'click', '2023-09-12 13:10:00')
]

df = spark.createDataFrame(data, schema)

#2.Renaming the column names
df = df.select(
    col("log_id").alias("log_id"),
    col("user_id").alias("user_id"),
    col("action").alias("user_activity"),
    col("timestamp").alias("time_stamp")
)
df.show()

# Convert timestamp column to timestamp type
df = df.withColumn("time_stamp", col("time_stamp").cast(TimestampType()))

# Print schema to verify the timestamp column type
print("DataFrame Schema:")
df.printSchema()

#3.Calculate the number of actions performed by each user in the last 7 days
print("Number of actions performed by each user in the last 7 days:")
df_last_7_days = df.filter(datediff(expr("date('2023-09-05')"), expr("date(timestamp)")) <= 7).groupBy("user_id").agg(count("*").alias("num_actions_last_7_days"))
df_last_7_days.show()

#4.Convert timestamp column to login_date column with YYYY-MM-DD format
df_timestamp = df.withColumn("login_date", df["time_stamp"].cast(DateType()))
df_timestamp.show()

spark.stop()
