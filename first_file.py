import findspark
findspark.init()

import os
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Create a Spark session
spark = SparkSession.builder \
    .appName("MySparkApp") \
    .getOrCreate()

current_script_directory = os.path.dirname(os.path.realpath(__file__))

file_name = 'dataset/organization.csv'
file_path = os.path.join(current_script_directory, file_name)


def read_csv_file(file_path: str)->DataFrame:
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    return df

df1 = read_csv_file(file_path)
df1.show(5)