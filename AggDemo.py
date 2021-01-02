import sys
from lib.logger import Log4J
from pyspark.sql import functions as f
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder \
                        .appName("AggDemo") \
                        .master("local[2]") \
                        .getOrCreate()

    logger = Log4J(spark)

    invoice_df = spark.read \
                      .format("csv") \
                      .option("header","true") \
                      .option("inferSchema","true") \
                      .load("data/invoices.csv")

    # To count number of records and sum of quantity.
    invoice_df.select(f.count("*").alias("Count *"),
              f.sum("Quantity").alias("TotalQuantity"),
              f.avg("UnitPrice").alias("AvgPrice"),
              f.countDistinct("InvoiceNo").alias("CountDistinct")).show()

    # Above can also be used as sql like string expressions as shown below
    invoice_df.selectExpr(
        "count(1) as `count 1`",
        "count(StockCode) as `count field`",
        "sum(Quantity) as `TotalQuantity`",
        "avg(UnitPrice) as `AvgPrice`"
    ).show()

    # Group Aggregation

    invoice_df.createOrReplaceTempView("sales")
    summary_sql = spark.sql(""" SELECT COUNTRY,InvoiceNo,
                            sum(Quantity) as TotalQuantity,
                            round(sum(Quantity * UnitPrice),2) as InvoiceValue
                        FROM sales
                        GROUP BY Country, InvoiceNo""")

    summary_sql.show()

    # Above group aggregation can be done using DATAFRAME expressions also

    summary_df = invoice_df \
                .groupBy("Country","InvoiceNo") \
                .agg(f.sum("Quantity").alias("TotalQuantity"),
                f.round(f.sum(f.expr("Quantity * UnitPrice")),2).alias("InvoiceValue"),
                  f.expr("round(sum(Quantity * UnitPrice),2) as InvoiceValue")
                )

    summary_df.show()