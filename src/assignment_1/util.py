from pyspark.sql.types import *
from pyspark.sql.functions import *
def create_purchase_data_df(spark):
    # Defining the schema for purchase_data_df
    purchase_schema = StructType([
        StructField("customer", IntegerType(), True),
        StructField("product_model", StringType(), True)
    ])

    # Create DataFrame for purchase_data_df
    purchase_data = [
        (1, "iphone13"),
        (1, "dell i5 core"),
        (2, "iphone13"),
        (2, "dell i5 core"),
        (3, "iphone13"),
        (3, "dell i5 core"),
        (1, "dell i3 core"),
        (1, "hp i5 core"),
        (1, "iphone14"),
        (3, "iphone14"),
        (4, "iphone13")
    ]

    return spark.createDataFrame(purchase_data, schema=purchase_schema)

def create_product_data_df(spark):
    # Defining the schema for product_data_df
    product_schema = StructType([
        StructField("product_model", StringType(), True)
    ])

    # Create DataFrame for product_data_df
    product_data = [
        ("iphone13",),
        ("dell i5 core",),
        ("dell i3 core",),
        ("hp i5 core",),
        ("iphone14",)
    ]

    return spark.createDataFrame(product_data, schema=product_schema)

def find_customers_with_only_iphone13(purchase_data_df, logger):
    iphone13_customers = purchase_data_df.filter(purchase_data_df['product_model'] == 'iphone13')
    iphone13_customers.show()
    logger.info("Successfully found customers who bought only iphone13.")

def find_customers_upgraded_to_iphone14(purchase_data_df, logger):
    iphone13_customers = purchase_data_df.filter(purchase_data_df['product_model'] == 'iphone13')
    iphone14_customers = purchase_data_df.filter(purchase_data_df['product_model'] == 'iphone14')
    upgraded_customers = iphone13_customers.join(iphone14_customers, "customer", "inner")
    upgraded_customers.distinct().show()
    logger.info("Successfully found customers who upgraded from iphone13 to iphone14.")

def find_customers_bought_all_products(purchase_data_df, product_data_df, logger):
    unique_product_models = product_data_df.select("product_model").distinct()
    customer_product_count = purchase_data_df.groupBy("customer").agg(countDistinct("product_model").alias("product_count"))
    customers_bought_all_products = customer_product_count.filter(customer_product_count["product_count"] == unique_product_models.count()).select("customer")
    customers_bought_all_products.show()
    logger.info("Successfully found customers who bought all products.")
