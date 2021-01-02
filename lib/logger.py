class Log4J:
    # We will require spark sessiion so we will take it as constructor argument
    def __init__(self, spark):
        # Now we will create log4j instance from spark session JVM
        log4j = spark._jvm.org.apache.log4j
        # Initialize log4j logger
        root_class = "spark.beginner"
        # in below we added application name along with root_class
        # log4J configuration works as long as base name matches.
        # But we donot want to hardcode application name so pass it to sparkSession
        # so lets got to config and get application name as below
        conf = spark.sparkContext.getConf()
        app_name = conf.get("spark.app.name")
        # self.logger = log4j.LogManager.getLogger(root_class + "." + "HelloSpark")
        # Value of spark.app.name comes from app_name used in sparksession creation of HelloSpark.py
        self.logger = log4j.LogManager.getLogger(root_class + "." +app_name)

        # To simplify more lets create one method for warning

    def warn(self, message):
        self.logger.warn(message)

    def info(self, message):
        self.logger.info(message)

    def error(self, message):
        self.logger.error(message)

    def debug(self, message):
        self.logger.debug(message)