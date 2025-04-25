from pyspark.sql import SparkSession
from pyspark.sql.functions import (to_timestamp, col, lower, expr,
                                   regexp_replace, regexp_extract, concat)
from pyspark.sql.types import StringType
import mysql.connector
from mysql.connector import Error

spark = (SparkSession.builder.appName("CarSales")
         .config("spark.driver.extraClassPath", "mysql-connector-j-9.0.0\mysql-connector-j-9.0.0.jar")
         .getOrCreate())

# Load in both csv files
carinfo_df = spark.read.csv("carinfo.csv", header=True, inferSchema=True)
saleinfo_df = spark.read.csv("saleinfo.csv", header=True, inferSchema=True)

"""
Data Cleaning
"""
# Remove rows with no vin number
carinfo_df = carinfo_df.dropna(subset=['vin'])
saleinfo_df = saleinfo_df.dropna(subset=['vin'])

# Remove Duplicates
carinfo_df = carinfo_df.dropDuplicates()
saleinfo_df = saleinfo_df.dropDuplicates()

# Lowercase all text
for c in carinfo_df.columns:
    if carinfo_df.schema[c].dataType == StringType():
        carinfo_df = carinfo_df.withColumn(c,lower(carinfo_df[c]))
for c in saleinfo_df.columns:
    if saleinfo_df.schema[c].dataType == StringType():
        saleinfo_df = saleinfo_df.withColumn(c,lower(saleinfo_df[c]))

corrections = {
    "chev truck": "chevrolet",
    "dodge tk": "dodge",
    "ford tk": "ford",
    "ford truck": "ford",
    "gmc truck": "gmc",
    "hyundai tk": "hyundai",
    "landrover": "land rover",
    "mazda tk": "mazda",
    "mercedes": "mercedes-benz",
    "mercedes-b": "mercedes-benz",
    "vw": "volkswagen"
}

carinfo_df = carinfo_df.replace(corrections, subset=["make"])
# Check the schema of both dfs
print(carinfo_df.schema)
print(saleinfo_df.schema)
carinfo_df.show()
# Rework the saledate column
# Convert to timestamp and standardize all time to UTC/GMT+0
saleinfo_df = (saleinfo_df.withColumn('saledate', regexp_replace(col('saledate'), r'\b(?:mon |tue |wed |thu |fri |sat |sun )\b', ""))
               .withColumn('gmt_direction', regexp_extract(col('saledate'), r'([+-])', 0))
               .withColumn('gmt_hours', concat(col('gmt_direction'),regexp_extract(col('saledate'), r'[+-](\d{2})', 1)).cast('integer'))
               .withColumn('gmt_minutes', concat(col('gmt_direction'),regexp_extract(col('saledate'), r'[+-]\d{2}(\d{2})', 1)).cast('integer'))
               .withColumn('gmt_minutes', (col('gmt_hours') * 60) + col('gmt_minutes'))
               .withColumn('saledate', regexp_replace(col('saledate'), r'( gmt).*',""))
               .withColumn('saledate', to_timestamp(col('saledate'),"MMM dd yyyy HH:mm:ss"))
               .withColumn('saledate', expr('saledate - INTERVAL 1 MINUTES * gmt_minutes'))
               )
saleinfo_df = saleinfo_df.drop(*['gmt_direction','gmt_hours','gmt_minutes'])


carsales_df = saleinfo_df.join(carinfo_df, 'vin')
carsales_df.show(truncate=False)

mysql_properties = {
    "user": "",
    "password": "",
    "driver": "com.mysql.cj.jdbc.Driver",
    "batchsize": "1000"
}


# # Create the mySQL server
try:
    connection = mysql.connector.connect(host='',user='',password='')
    if connection.is_connected():
        cursor = connection.cursor()

        cursor.execute('CREATE DATABASE IF NOT EXISTS cars')
        print('Database creation Successful')

except Error as e:
    print(f"Error: {e}")
finally:
    if connection.is_connected():
        cursor.close()
        connection.close()

carsales_df = carsales_df.repartition(10)

carsales_df.write.jdbc(url="jdbc:mysql://",table='',mode='',properties=mysql_properties)
