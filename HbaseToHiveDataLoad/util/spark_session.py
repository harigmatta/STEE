from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def spark_session(table_name, load_type, biz_date):
    table_name = table_name
    load_type = load_type
    biz_date = biz_date
    app_name = load_type + "_" + table_name + biz_date
    spark = SparkSession.builder.appName(app_name) \
        .enableHiveSupport() \
        .getOrCreate()
    return spark

