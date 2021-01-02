import sys

import logger as logger
from  pyspark.sql import *
from pyspark import SparkConf
from lib.logger import Log4J
from lib.utils import load_survey_df, count_by_country

if __name__ == "__main__":
    print("Create SPARK SESSION")
# Below command will give spark session object.
# Any application will have only one active spark session. Spark session is configurable
# builder method will return builder object which allows you to configure your session and finally use getOrCreate

    Passing parameter without using SparkConf object.
     spark = SparkSession.builder \
        .appName("HelloSpark") \
        .master("local[3]") \
        .getOrCreate()
    # End of passing parameter without using SparkConf object

   # Below code is hardcoded we can avoid it by using specific code outside application code
    # e.g spark.master should be outside application code First approach is to keep it in separate file and load it at runtime
    # 
    #conf = SparkConf()
    #conf.set("spark.app.name","HelloSpark")
    #conf.set("spark.master","local[3]")
    #spark = SparkSession.builder \
     #       .config(conf=conf) \
      #      .getOrCreate()

# With above command  we created spark driver
# Now we can process data and once it is complete stop driver
    logger = Log4J(spark)
    logger.info("Start logging")
    # Check command line arguments
    if len(sys.argv) != 2:
        # log error
        logger.error("Usage: HelloSpark <filename>")
        # Exit
        sys.exit(-1)

    # Below read method will return DataFrame reader object.
    survey_df = load_survey_df(spark, sys.argv[1])
    print("************************Full Data****************************************")
    survey_df.show()
    # To add filter criteria and show selected columns
    survey_filter_df = survey_df \
        .where("Age < 40") \
        .select("Age", "Gender", "Country", "state")
    grouped_df = survey_filter_df.groupBy("Country")
    print("************************Filtered and Grouped Data****************************************")
    # count_df = grouped_df.count()
    # To simulate the multiple partition structure of cluster environmnet lets repartition it
    partitioned_survey_df = survey_df.repartition(2)
    count_df = count_by_country(partitioned_survey_df)

    # count_df.show()
    # Replacing above show action with below logger method because COLLECT method will return DATAFRAME as PYTHON LIST
    # However SHOW method is Utility function to print DATAFRAME. SHOW can be used for debugging purpose.
    logger.info(count_df.collect())
    input("Press Enter")
    # logger.info("Finished logging")
    # print("Coding complete")
    # spark.stop()



