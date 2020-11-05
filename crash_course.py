from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark.sql.functions as sqlfunc

spark = SparkSession.builder.getOrCreate()

# set column data types before loading
schema = StructType([
    StructField('id', IntegerType()),
    StructField('first_name', StringType()),
    StructField('last_name', StringType()),
    StructField('gender', StringType()),
    StructField('City', StringType()),
    StructField('JobTitle', StringType()),
    StructField('Salary', StringType()),
    StructField('Latitude', FloatType()),
    StructField('Longitude', FloatType())
])

df = spark.read.csv("original.csv", header=True, schema=schema)

# drop all rows with an na
df_dropped = df.na.drop()

# drop all rows will null JobTitle
df_null_jobs = df.filter(df.JobTitle.isNotNull())

# replace null City with 'Unknown'
df_handled = df.withColumn("clean_city", when(df.City.isNull(), 'Unknown').otherwise(df.City))

# drop dupes
df_deduped = df.dropDuplicates()

# select multiple cols
df_select = df.select("first_name", "last_name")

# rename cols
df_renamed = df.withColumnRenamed('JobTitle', 'job_title')

# filter
df_filter = df.filter((df.first_name == 'Thain'))

df_filter = df.filter((df.first_name.like("%ai%")))

df_filter = df.filter((df.first_name.endswith("ain")))

df_filter = df.filter((df.first_name.startswith("Al")))

df_filter = df.filter((df.id.between(1, 5)))

df_filter = df.filter((df.first_name.isin('Shannon', 'Alvera')))

# substrings
df_substr = df.select(df.first_name, df.first_name.substr(1, 5).alias('name'))

# multiple filters
df_filters = df.filter((df.first_name.isin('Aldin', 'Valma')) | (df.City.like('%ondon')))

df_filters = df.filter((df.id > 10) & (df.id < 100))

# create temp table to run SQL queries on
df.registerTempTable("original")

query1 = spark.sql('select * from original')
# query1.show()

query2 = spark.sql("""select concat(first_name, ' ', last_name) as name 
                     from original where gender = 'Female'""")
# query2.show()

# calculated cols
df_calc = df.withColumn('clean_salary', df.Salary.substr(2, 100).cast('float'))

df_calc = df_calc.withColumn('monthly_salary', df_calc.clean_salary / 12)

# new binary col
df_vp = df.withColumn('vp_role', when(df.JobTitle.like("VP%"), 'Yes').otherwise('No'))

# aggregation
df1 = df_calc.groupBy('gender').agg(sqlfunc.sum('clean_salary'))

df2 = df_calc.groupBy('gender', 'city').agg(sqlfunc.sum('clean_salary').alias('total'),
                                            sqlfunc.avg('clean_salary').alias('average'),
                                            sqlfunc.min('clean_salary').alias('min'),
                                            sqlfunc.max('clean_salary').alias('max'))

# write to file
df2.write.csv("test.csv")