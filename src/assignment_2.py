from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("Credit Card Dataframe") .getOrCreate()
#1.Create a Dataframe as credit_card_df with different read methods
data = [("1234567891234567",),
        ("5678912345671234",),
        ("9123456712345678",),
        ("1234567812341122",),
        ("1234567812341342",)]

credit_card_df = spark.createDataFrame(data, ["card_number"])
credit_card_df.show()
#Another method
schema = StructType([
StructField("card_number", StringType(), nullable=False)
])
credit_card_df_schema = spark.createDataFrame(data, schema=schema)
credit_card_df_schema.show()
#from a list of row
credit_card_list= spark.createDataFrame(data)
credit_card_list.show()

#2.print number of partitions
num_partitions =credit_card_df.rdd.getNumPartitions()
print("Number of partitions:", num_partitions)

#3.Increase the partition size to 5
credit_card_df_repartition= credit_card_df.repartition(5)#repartition is to increase or decrease the partition
#4.Decrease the partition size back to its original partition size
credit_card_df_coalesce= credit_card_df.coalesce(credit_card_df.rdd.getNumPartitions())

# 5. Create a UDF to mask the credit card number
def mask_card_number(card_number):
    masked_number = '*' * 12 + card_number[-4:]
    return masked_number

mask_card_number_udf = udf(mask_card_number, StringType())

# 6. Apply the UDF to create a new column with masked card numbers
credit_card_df_masked= credit_card_df.withColumn("masked_card_number", mask_card_number_udf(col("card_number")))
credit_card_df_masked.show()