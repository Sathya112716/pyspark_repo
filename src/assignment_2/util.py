from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType


def create_credit_card_dataframe(spark):
    data = [("1234567891234567",),
            ("5678912345671234",),
            ("9123456712345678",),
            ("1234567812341122",),
            ("1234567812341342",)]

    return spark.createDataFrame(data, ["card_number"])


def get_num_partitions(df):
    return df.rdd.getNumPartitions()


def increase_partitions(df, num_partitions):
    return df.repartition(num_partitions)


def decrease_partitions(df):
    return df.coalesce(df.rdd.getNumPartitions())


def mask_credit_card_numbers(df):
    def mask_card_number(card_number):
        masked_number = '*' * 12 + card_number[-4:]
        return masked_number

    mask_card_number_udf = udf(mask_card_number, StringType())

    return df.withColumn("masked_card_number", mask_card_number_udf(col("card_number")))
