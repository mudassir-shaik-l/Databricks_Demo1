# print("Hello")

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# Create a Spark session
spark = SparkSession.builder \
    .appName("Sample Orders DataFrame") \
    .getOrCreate()

# Define the schema
schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("order_date", StringType(), True),
    StructField("status", StringType(), True),
    StructField("amount", FloatType(), True)
])

# Sample data
data = [
    (1, 101, "2025-08-01", "shipped", 250.50),
    (2, 102, "2025-08-02", "delivered", 120.00),
    (3, 103, "2025-08-03", "pending", 75.75),
    (4, 104, "2025-08-04", "shipped", 300.20),
    (5, 105, "2025-08-05", "cancelled", 0.00),
    (6, 106, "2025-08-06", "delivered", 199.99),
    (7, 107, "2025-08-07", "pending", 89.99),
    (8, 108, "2025-08-08", "shipped", 400.00),
    (9, 109, "2025-08-09", "delivered", 149.49),
    (10, 110, "2025-08-10", "returned", 0.00)
]

# Create DataFrame
orders_df = spark.createDataFrame(data, schema)

# Show DataFrame
orders_df.show()

# Delete the DataFrame
orders_df.unpersist()
del orders_df


# Duplicate the DataFrame
orders_df_copy = spark.createDataFrame(data, schema)
orders_df_copy.show()

# Duplicate the DataFrame using the .cache() and .select("*") method
orders_df_duplicate = orders_df.select("*")
orders_df_duplicate.show()

