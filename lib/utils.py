def load_survey_df(spark,data_file):
    return spark.read  \
            .option("header","true")    \
            .option("inferSchema", "true")  \
            .csv(data_file)

def count_by_country(survey_df):
    return survey_df \
           .where("Age < 40")  \
           .select("Age","Gender","Country","state")    \
           .groupBy("Country")  \
           .count()

