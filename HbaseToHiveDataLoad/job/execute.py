from pyspark.sql.functions import *


def query_hbase_table(spark, schema, table_name, load_type, query, filter_col, start_date_epoch, end_date_epoch):
    separator = ","
    try:
        hbase_df = spark.read.format("org.apache.hadoop.hbase.spark") \
            .option("hbase.columns.mapping", schema) \
            .option("hbase.table", table_name) \
            .option("hbase.spark.use.hbasecontext", False).load()

        hbase_df.createOrReplaceTempView(table_name)

        query.replace("from", "{1} from_unixtime(%s, 'yyyy-mm-dd') as %s from".format(filter_col, separator))
        final_query = query + " where {0} >= {1} and {0} <= {1}".format(filter_col, start_date_epoch, end_date_epoch)
        print("Final Query: %s".format(final_query))
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
