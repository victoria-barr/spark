from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import lit
import pyspark.sql.functions as f

import numpy as np

# start using spark?
spark = SparkSession.builder.getOrCreate()

# read in csv
df = spark.read.csv("C:\\original.csv", header="true")

# clean City in new col
df2 = df.withColumn("clean_city", when(df.City.isNull(), 'Unknown').otherwise(df.City))

# subset to no null Job Titles
df2 = df2.filter(df2.JobTitle.isNotNull())

# cast Salary col as float
df2 = df2.withColumn("clean_salary", df2.Salary.substr(2, 100).cast('float'))

# find mean salary
mean = df2.groupBy().avg('clean_salary').take(1)[0][0]

# if null salary, impute mean
df2 = df2.withColumn("new_salary", when(df2.clean_salary.isNull(), lit(mean)).otherwise(df2.clean_salary))

# latitudes col only
latitudes = df2.select('Latitude')

latitudes = latitudes.filter(latitudes.Latitude.isNotNull())

# cast lats as float
latitudes = latitudes.withColumn("latitude2", latitudes.Latitude.cast('float')).select('latitude2')

# median lat
median = np.median(latitudes.collect())

# when Latitude col is null, use median lat
df2 = df2.withColumn('lat', when(df2.Latitude.isNull(), lit(median)).otherwise(df2.Latitude))

# salary by gender
genders = df2.groupBy('gender').agg(f.avg('new_salary').alias("AvgSalary"))

gender_df = df2.withColumn("female_salary", when(df2.gender == 'Female', df2.new_salary).otherwise(lit(0)))

gender_df = gender_df.withColumn("male_salary", when(gender_df.gender == 'Male', gender_df.new_salary).otherwise(lit(0)))

gender_df = gender_df.groupBy('JobTitle').agg(f.avg('female_salary').alias('final_female_salary'),
                                  f.avg('male_salary').alias('final_male_salary'))

gender_df.show()