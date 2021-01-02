import sys
from pyspark.sql import *
from lib.logger import Log4J

if __name__ == "__main__":
    spark = SparkSession.builder \
                        .master("local[3]") \
                        .appName("SparkSQLTableDemo") \
                        .enableHiveSupport() \
                        .getOrCreate()

    logger = Log4J(spark)
    flightTimeParquetDF = spark.read \
                                .format("parquet") \
                                .load("data/flight*.parquet")

    spark.sql("CREATE DATABASE IF NOT EXISTS AIRLINE_DB")
    spark.catalog.setCurrentDatabase("AIRLINE_DB")
    flightTimeParquetDF.write \
                       .mode("overwrite") \
                        .partitionBy("ORIGIN","OP_CARRIER") \
                        .saveAsTable("fligh_data_tbl")

     logger.info(spark.catalog.listTables("AIRLINE_DB"))

    # Above commands will create two new directories metastore_db and spark-warehouse
    # We have parition it by column having 150 plus unique values to handle such scenario BUCKETING is used.

    flightTimeParquetDF.write \
                        .format("csv")  \
                        .mode("overwrite") \
                        .bucketBy(5,"ORIGIN","OP_CARRIER") \
                        .sortBy("ORIGIN","OP_CARRIER") \
                        .saveAsTable("flight_data_tbl")


    # saveAsTable method will take tablename and create managed table in current spark database.
    # Above table will be created in DEFAULT database.
