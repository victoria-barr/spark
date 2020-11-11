# challenge from Crash Course
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as sqlfunc

spark = SparkSession.builder.getOrCreate()

schema = StructType([
  StructField('ip_address', StringType()),
  StructField('Country', StringType()),
  StructField('Domain Name', StringType()),
  StructField('Bytes_used', IntegerType())
])

# read in csv
df = spark.read.csv("challenge.csv", header=True, schema=schema)

# rename cols
df = df.withColumnRenamed("Domain Name", "domain_name")\
  .withColumnRenamed("Bytes_used", "bytes_used")\
  .withColumnRenamed("Country", "country")

# add col to say if country is Mexico (y/n)
df = df.withColumn("mexico_binary", when(df.country == 'Mexico', 'Yes').otherwise('No'))

# group by mexico_binary col and sum bytes used
df1 = df.groupBy('mexico_binary').agg(sqlfunc.sum('bytes_used').alias('agg_bytes'))
df1.show()

# group by country, count IP addresses in each country
df2 = df.groupBy('country').agg(sqlfunc.countDistinct('ip_address').alias('n_ip_addresses'))
df2.show()
