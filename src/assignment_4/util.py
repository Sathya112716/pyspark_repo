from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, explode_outer, posexplode, current_date, year, month, day
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


def read_json_file(spark, file_path):
    return spark.read.json("C:\\Users\\SathyaPriyaR\\Desktop\\pyspark_ass\\pyspark_repo\\resources\\nested_json_file (2).json")


def flatten_dataframe(df):
    # Define custom schema
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("store_size", StringType(), True),
        StructField("employee_id", IntegerType(), True),
        StructField("employee_name", StringType(), True)
    ])
    # Flatten the DataFrame using explode
    flattened_df = df.select(
        col("id"),
        col("properties.name").alias("name"),
        col("properties.storeSize").alias("store_size"),
        explode("employees").alias("employee")
    ).select(
        col("id"),
        col("name"),
        col("store_size"),
        col("employee.empId").alias("employee_id"),
        col("employee.empName").alias("employee_name")
    ).withColumn("employee_id", col("employee_id").cast(IntegerType()))

    return flattened_df


def count_records(df):
    return df.count()


def differentiate_with_explode(df):
    # Explode
    df_explode = df.select(col("*"), explode("employees").alias("employee"))
    # Explode outer
    df_explode_outer = df.select(col("*"), explode_outer("employees").alias("employee_outer"))
    # Posexplode
    df_posexplode = df.select(col("*"), posexplode("employees").alias("pos", "employee_pos"))

    return df_explode, df_explode_outer, df_posexplode


def filter_by_id(df, id_value):
    return df.filter(col("id") == id_value)


def convert_camel_to_snake_case(df):
    # Convert column names from camel case to snake case
    snake_case_df = df.toDF(*[col_name.lower() for col_name in df.columns])
    return snake_case_df


def add_load_date_column(df):
    return df.withColumn("load_date", current_date())


def extract_year_month_day(df):
    return df.withColumn("year", year("load_date")).withColumn("month", month("load_date")).withColumn("day",
                                                                                                       day("load_date"))
