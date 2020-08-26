# Databricks notebook source
## Unmounting a folder
dbutils.fs.unmount("/mnt/s3data")

# COMMAND ----------

import urllib
ACCESS_KEY = ""
SECRET_KEY = ""
AWS_BUCKET_NAME = "robin-databricks-test1"
MOUNT_NAME = "s3data"
dbutils.fs.mount("s3n://%s:%s@%s" % (ACCESS_KEY, SECRET_KEY, AWS_BUCKET_NAME), "/mnt/%s" % MOUNT_NAME)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/s3data"))

# COMMAND ----------

# File Location and type
file_location = "dbfs:/mnt/s3data/listings.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for csv files. For othe file tyoes, these will be ignored
df_listing =  spark.read.format(file_type) \
                .option("inferSchema", infer_schema) \
                .option("header", first_row_is_header) \
                .option("sep", delimiter) \
                .load(file_location)
display(df_listing)


# COMMAND ----------

# File Location and type
file_location = "dbfs:/mnt/s3data/listings.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for csv files. For othe file tyoes, these will be ignored
df_listing =  spark.read.format(file_type) \
                .option("multiline", "true") \
                .option("inferSchema", infer_schema) \
                .option("header", first_row_is_header) \
                .option("sep", delimiter) \
                .load(file_location)
display(df_listing)


# COMMAND ----------

# File Location and type
file_location = "dbfs:/mnt/s3data/listings.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for csv files. For othe file tyoes, these will be ignored
df_listing =  spark.read.format(file_type) \
                .option("multiline", "true") \
                .option("quote", "\"") \
                .option("escape", "\"") \
                .option("inferSchema", infer_schema) \
                .option("header", first_row_is_header) \
                .option("sep", delimiter) \
                .load(file_location)
display(df_listing)


# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC drop table listing

# COMMAND ----------

spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")

listing_table_name = "listing"
df_listing.write.format("parquet").saveAsTable(listing_table_name)

# COMMAND ----------

## Columns data type
df_listing.printSchema()

# COMMAND ----------

##
from pyspark.sql.types import *

df_listing = df_listing.withColumn("scrape_id", col("scrape_id").cast(DoubleType()))

# COMMAND ----------

df_listing.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from listing

# COMMAND ----------

##
# File Location and type
file_location = "dbfs:/mnt/s3data/neighbourhoods.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

neighbourhoodSchema = StructType([
  StructField("neighbourhood_group", StringType(), True),
  StructField("neighbourhood", StringType(), True)
])

# The applied options are for csv files. For other file tyoes, these will be ignored
df_neighbourhoods =  spark.read.format(file_type) \
                .option("wholeFile", "true") \
                .option("multiline", "true") \
                .option("schema", neighbourhoodSchema) \
                .option("header", first_row_is_header) \
                .option("sep", delimiter) \
                .load(file_location)
display(df_neighbourhoods)

# COMMAND ----------

listing_table_name = "neighbourhood"
df_neighbourhoods.write.mode("overwrite").format("parquet").saveAsTable(listing_table_name)

# COMMAND ----------

# File Location and type
file_location = "dbfs:/mnt/s3data/reviews.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for csv files. For othe file tyoes, these will be ignored
df_reviews =  spark.read.format(file_type) \
                .option("inferSchema", infer_schema) \
                .option("header", first_row_is_header) \
                .option("sep", delimiter) \
                .load(file_location)
display(df_reviews)


# COMMAND ----------

# File Location and type
file_location = "dbfs:/mnt/s3data/reviews.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for csv files. For othe file tyoes, these will be ignored
df_reviews =  spark.read.format(file_type) \
                .option("wholeFile", "true") \
                .option("multiline", "true") \
                .option("inferSchema", infer_schema) \
                .option("header", first_row_is_header) \
                .option("sep", delimiter) \
                .load(file_location)
display(df_reviews)

# COMMAND ----------

##
display(spark.read.text("dbfs:/mnt/s3data/reviews.csv"))

# COMMAND ----------

temp_df = spark.read.text("dbfs:/mnt/s3data/reviews.csv")

# COMMAND ----------

header = temp_df.head(1)

# COMMAND ----------

##
fixed_df = temp_df.filter(temp_df.value.contains("20"))

# COMMAND ----------

fixed_df.count()

# COMMAND ----------

display(fixed_df)

# COMMAND ----------

final_df = spark.createDataFrame(header).union(fixed_df)
final_df.show()

# COMMAND ----------

final_df.repartition(1).write.text("dbfs:/mnt/s3data/reviews_fixed")

# COMMAND ----------

display(dbutils.fs.ls("/mnt/s3data"))

# COMMAND ----------

# File Location and type
file_location = "dbfs:/mnt/s3data/reviews_fixed/"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for csv files. For othe file tyoes, these will be ignored
df_reviews =  spark.read.format(file_type) \
                .option("inferSchema", infer_schema) \
                .option("header", first_row_is_header) \
                .option("sep", delimiter) \
                .load(file_location)
display(df_reviews)

# COMMAND ----------

listing_table_name = "reviews"
df_reviews.write.format("parquet").saveAsTable(listing_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from listing

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from listing

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from neighbourhood

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from reviews

# COMMAND ----------

# MAGIC %md ## What property types are available in airbnb listing and Distribution of those?

# COMMAND ----------

display(df_listing.groupBy('property_type').count())

# COMMAND ----------

from pyspark.sql.functions import *

df_listing.groupBy('property_type').count().orderBy(col('count').desc()).show()

# COMMAND ----------

# MAGIC %md ## What is average price across each property type?

# COMMAND ----------

df_listing.groupBy("property_type", "price").agg(({"price": "average"})).show()

# COMMAND ----------

df_listing.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct price from listing

# COMMAND ----------

from pyspark.sql.types import FloatType

def trim_char(string):
  return string.strip("$")

spark.udf.register("trim_func", trim_char)

trim_func_udf = udf(trim_char)

# COMMAND ----------

chk = df_listing.select("property_type", "price", trim_func_udf("price").cast(FloatType()).alias("price_f"))
chk.groupBy("property_type").agg({"price_f":"average"}).show()

# COMMAND ----------


