from pyspark.sql.functions import *
import datetime
import calendar


def get_timestamp_epoch(load_type, biz_date):
    if load_type == "Historical":
        year_dt = int(biz_date)
        start_epoch = int(calendar.timegm(datetime.datetime(year_dt, 1, 1, 0, 0, 0).timetuple()))
        end_epoch = int(calendar.timegm(datetime.datetime(year_dt, 12, 31, 23, 59, 59).timetuple()))
        return start_epoch, end_epoch
    else:
        year_dt = int(biz_date.split("-")[0])
        month_dt = int(biz_date.split("-")[1])
        day = int(biz_date.split("-")[2])
        start_epoch = int(calendar.timegm(datetime.datetime(year_dt, month_dt, day, 0, 0, 0).timetuple()))
        end_epoch = int(calendar.timegm(datetime.datetime(year_dt, month_dt, day, 23, 59, 59).timetuple()))
        return start_epoch, end_epoch


def query_hbase_table(spark, schema, table_name, logger, query, filter_col, start_date_epoch, end_date_epoch):
    separator = ","
    try:
        hbase_df = spark.read.format("org.apache.hadoop.hbase.spark") \
            .option("hbase.columns.mapping", schema) \
            .option("hbase.table", table_name) \
            .option("hbase.spark.use.hbasecontext", False).load()

        hbase_df.createOrReplaceTempView(table_name)
        partition_col_append_query = query.replace("from", "{1} from_unixtime({0}, 'yyyy-mm-dd') as {0} from".format(filter_col, separator))
        logger.info("Part Query: ", partition_col_append_query)
        final_query = partition_col_append_query + " where {0} >= {1} and {0} <= {2}".format(filter_col, start_date_epoch, end_date_epoch)
        logger.info("Final Query: {}".format(final_query))
        if len(final_query) != 0:
            final_df = spark.sql(final_query)
            return final_df
        else:
            logger.error("Error: The final query cannot be null")
            exit(1)
    except Exception as e:
        logger.error("Job Failed due to {}".format(e))
        sys.exit(1)


def write_hive_table(logger, final_df, db_name, table_name):
    target_table = db_name + "." + table_name
    logger.info("Write to Hive table: {}".format(target_table))
    final_df.write.format("parquet").mode("overwrite").insertInto(target_table)
