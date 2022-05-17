from pyspark.sql.functions import *
import datetime
import calendar


def get_timestamp_epoch(load_type, biz_date):
    if load_type == "Historical":
        year = int(biz_date)
        start_epoch = int(calendar.timegm(datetime.datetime(year, 1, 1, 0, 0, 0).timetuple()))
        end_epoch = int(calendar.timegm(datetime.datetime(year, 12, 31, 23, 59, 59).timetuple()))
        return start_epoch, end_epoch
    else:
        year = int(biz_date.split("-")[0])
        month = int(biz_date.split("-")[1])
        day = int(biz_date.split("-")[2])
        start_epoch = int(calendar.timegm(datetime.datetime(year, month, day, 0, 0, 0).timetuple()))
        end_epoch = int(calendar.timegm(datetime.datetime(year, month, day, 23, 59, 59).timetuple()))
        return start_epoch, end_epoch


def query_hbase_table(spark, schema, table_name, load_type, query, filter_col, start_date_epoch, end_date_epoch):
    separator = ","
    try:
        hbase_df = spark.read.format("org.apache.hadoop.hbase.spark") \
            .option("hbase.columns.mapping", schema) \
            .option("hbase.table", table_name) \
            .option("hbase.spark.use.hbasecontext", False).load()

        hbase_df.createOrReplaceTempView(table_name)
        partition_col_append_query = query.replace("from", "{1} from_unixtime({0}, 'yyyy-mm-dd') as {0} from".format(filter_col, separator))
        print("Part Query: ", partition_col_append_query)
        final_query = partition_col_append_query + " where {0} >= {1} and {0} <= {2}".format(filter_col, start_date_epoch, end_date_epoch)
        print("Final Query: {}".format(final_query))
        if len(final_query) != 0:
            final_df = spark.sql(final_query)
            return final_df
        else:
            print("Error: The final query cannot be null")
            exit(1)
    except Exception as e:
        print(e)
        sys.exit(1)


def write_hive_table(final_df, db_name, table_name):
    target_table = db_name + "." + table_name
    final_df.write.format("parquet").mode("overwrite").insertInto(target_table)
