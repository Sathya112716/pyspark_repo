from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import *

# Create SparkSession
spark = SparkSession.builder.appName("Customer Product Purchase").getOrCreate()

#1.Create DataFrame as purchase_data_df,  product_data_df with custom schema
# Defining the schema for purchase_data_df
purchase_schema = StructType([
    StructField("customer", IntegerType(), True),
    StructField("product_model", StringType(), True)
])

# Defining the schema for product_data_df
product_schema = StructType([
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

purchase_data_df = spark.createDataFrame(purchase_data, schema=purchase_schema)
purchase_data_df.show()

# Create DataFrame for product_data_df
product_data = [
    ("iphone13",),
    ("dell i5 core",),
    ("dell i3 core",),
    ("hp i5 core",),
    ("iphone14",)
]

product_data_df = spark.createDataFrame(product_data, schema=product_schema)
product_data_df.show()


#2.Find the customers who have bought only iphone13
iphone13_customers = purchase_data_df.filter(purchase_data_df['product_model'] == 'iphone13')
iphone13_customers.show()

#3.Find customers who upgraded from product iphone13 to product iphone14
iphone13_customers = purchase_data_df.filter(purchase_data_df['product_model'] == 'iphone13')
iphone14_customers = purchase_data_df.filter(purchase_data_df['product_model'] == 'iphone14')
iphone14_customers.show()
upgraded_customers=iphone13_customers.join(iphone14_customers,"customer","inner")
upgraded_customers.distinct().show()

#4.Find customers who have bought all models in the new Product Data

unique_product_models = product_data_df.select("product_model").distinct()
unique_product_models.show()
customer_product_count = purchase_data_df.groupBy("customer").agg(countDistinct("product_model").alias("product_count"))
customer_product_count.show()
customers_bought_all_products = customer_product_count.filter(customer_product_count["product_count"] == unique_product_models.count()).select("customer")
customers_bought_all_products.show()


