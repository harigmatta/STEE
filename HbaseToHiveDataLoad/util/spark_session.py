from pyspark.sql import SparkSession
import datetime


def spark_session(table_name, load_type, biz_date):
    table_name = table_name
    load_type = load_type
    biz_date = biz_date
    app_name = load_type + "_" + table_name + biz_date
    spark = SparkSession.builder.appName(app_name) \
        .enableHiveSupport() \
        .getOrCreate()
    return spark


def get_timestamp_epoch(load_type, biz_date):
    if load_type == "Historical":
        year = int(biz_date)
        start_epoch = int(datetime.datetime(year, 1, 1, 0, 0, 0).timestamp())
        end_epoch = int(datetime.datetime(year, 12, 31, 23, 59, 59).timestamp())
        return start_epoch, end_epoch
    else:
        year = int(biz_date.split("-")[0])
        month = int(biz_date.split("-")[1])
        day = int(biz_date.split("-")[2])
        start_epoch = int(datetime.datetime(year, month, day, 0, 0, 0).timestamp())
        end_epoch = int(datetime.datetime(year, month, day, 23, 59, 59).timestamp())
        return start_epoch, end_epoch
