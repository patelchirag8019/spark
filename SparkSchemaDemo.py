import sys

from pyspark.sql.types import StructType, DateType, StringType, IntegerType, StructField

from lib.logger import Log4J
from pyspark.sql import *

if __name__ == "__main__":
    spark = SparkSession.builder \
            .master("local[3]") \
            .appName("SparkSchemaDemo") \
            .getOrCreate()

    logger = Log4J(spark)

    # Lets define schema for below data using programmatic method
    # STRUCT type represents a DATAFRAME ROW structure and  STRUCT Field is column definition
    flightSchemaStruct = StructType([
        StructField("FL_DATE", DateType()),
        StructField("OP_CARRIER", StringType()),
        StructField("OP_CARRIER_FL_NUM", IntegerType()),
        StructField("ORIGIN", StringType()),
        StructField("ORIGIN_CITY_NAME", StringType()),
        StructField("DEST", StringType()),
        StructField("DEST_CITY_NAME", StringType()),
        StructField("CRS_DEP_TIME", IntegerType()),
        StructField("DEP_TIME", IntegerType()),
        StructField("WHEELS_ON", IntegerType()),
        StructField("TAXI_IN", IntegerType()),
        StructField("CRS_ARR_TIME", IntegerType()),
        StructField("ARR_TIME", IntegerType()),
        StructField("CANCELLED", IntegerType()),
        StructField("DISTANCE", IntegerType())
    ])

    print("******************Output After Defining STRUCT Type***************")

    flightTimeStructDF = spark.read \
                            .format("csv") \
                            .option("header","true") \
                            .schema(flightSchemaStruct) \
                            .option("mode","FAILFAST") \
                            .option("dateFormat","M/d/y") \
                            .load("data/flight*.csv")
    flightTimeStructDF.show()
    logger.info("STRUCT TYPE Schema:" + flightTimeStructDF.schema.simpleString())

    print("******************End of Publishing Output After Defining STRUCT Type***************")

    # In below we are not giving any Schema Nor we are asking DataFrame to InferSchema
    flightTimeCsvDF =  spark.read \
                        .format("csv") \
                        .option("header","true") \
                        .load("data/flight*.csv")

    print("******************Output With Default OPTION***************")
    flightTimeCsvDF.show(5)
    logger.info("CSV Schema:" + flightTimeCsvDF.schema.simpleString())

    # Output of above command shows as all column datatype as STRING lets use INFERSCHEMA option

    print("******************Output Using INFERSCHEMA OPTION***************")
    flightTimeCsvDF_inf = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("data/flight*.csv")
    flightTimeCsvDF_inf.show(5)
    logger.info("Infer Schema:" + flightTimeCsvDF_inf.schema.simpleString())

    # Output of above command shows as all column is not correctly detecting DATE datatype lets use JSON option

    print("******************Output Using JSON OPTION***************")
    flightTimeJsonDF = spark.read \
        .format("json") \
        .load("data/flight*.json")
    flightTimeJsonDF.show(5)
    logger.info("Json Schema:" + flightTimeJsonDF.schema.simpleString())

    print("******************Output Using PARQUET File***************")
    flightTimeJsonDF = spark.read \
        .format("parquet") \
        .load("data/flight*.parquet")
    flightTimeJsonDF.show(5)
    logger.info("PARQUET Schema:" + flightTimeJsonDF.schema.simpleString())

    print("******************End of Publishing Output After USING PARQUET File***************")

    print("******************Output After Defining DDL Schema***************")

    flightSchemaDDL = """FL_DATE DATE, OP_CARRIER STRING, OP_CARRIER_FL_NUM INT, ORIGIN STRING, 
          ORIGIN_CITY_NAME STRING, DEST STRING, DEST_CITY_NAME STRING, CRS_DEP_TIME INT, DEP_TIME INT, 
          WHEELS_ON INT, TAXI_IN INT, CRS_ARR_TIME INT, ARR_TIME INT, CANCELLED INT, DISTANCE INT"""

    flightSchemaDDLDF = spark.read \
                             .format("json") \
                             .option("header", "true") \
                             .schema(flightSchemaDDL) \
                             .option("dateFormat", "M/d/y") \
                             .load("data/flight*.json")

    flightSchemaDDLDF.show(5)
    logger.info("Json Schema:" + flightSchemaDDLDF.schema.simpleString())

print("******************End of Publishing Output After Defining DDL Schema***************")

