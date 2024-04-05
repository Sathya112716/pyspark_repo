import logging
from pyspark.sql import SparkSession
from util import *

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize SparkSession
spark = SparkSession.builder.appName("JSON Data Analysis").getOrCreate()

# Read JSON file
json_df = read_json_file(spark, "your_json_file_path.json")

# Flatten the DataFrame
flattened_df = flatten_dataframe(json_df)

# Find record count before and after flattening
original_count = count_records(json_df)
flattened_count = count_records(flattened_df)
difference = flattened_count - original_count
logger.info("Record count before flattening: %d", original_count)
logger.info("Record count after flattening: %d", flattened_count)
logger.info("Difference in record count: %d", difference)

# Differentiate using explode functions
df_explode, df_explode_outer, df_posexplode = differentiate_with_explode(json_df)

# Filter by id
filtered_df = filter_by_id(json_df, 1001)

# Convert column names from camel case to snake case
snake_case_df = convert_camel_to_snake_case(json_df)

# Add load_date column
df_with_load_date = add_load_date_column(json_df)

# Extract year, month, and day from load_date column
df_with_date_columns = extract_year_month_day(df_with_load_date)

# Stop SparkSession
spark.stop()
