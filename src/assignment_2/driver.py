import logging
from pyspark.sql import SparkSession
from util import create_credit_card_dataframe, get_num_partitions, increase_partitions, decrease_partitions, mask_credit_card_numbers

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create SparkSession
spark = SparkSession.builder.appName("Credit Card Dataframe").getOrCreate()

# Create credit_card_df using different read methods
credit_card_df = create_credit_card_dataframe(spark)

# Print number of partitions
num_partitions = get_num_partitions(credit_card_df)
logger.info("Number of partitions: %d", num_partitions)

# Increase the partition size to 5
credit_card_df_repartition = increase_partitions(credit_card_df, 5)

# Decrease the partition size back to its original partition size
credit_card_df_coalesce = decrease_partitions(credit_card_df_repartition)

# Mask credit card numbers using UDF function
credit_card_df_masked = mask_credit_card_numbers(credit_card_df)

# Show the masked credit card DataFrame
credit_card_df_masked.show()
