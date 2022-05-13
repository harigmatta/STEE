from pyspark.sql.functions import *


def query_hbase_table(spark, schema, table_name, load_type, query, filter_col, business_date):
    try:
        final_query = ""
        hbase_df = spark.read.format("org.apache.hadoop.hbase.spark") \
            .options(catalog=schema) \
            .option("hbase.spark.use.hbasecontext", False).load()
        hbase_df.createOrReplaceTempView(table_name)
        if load_type == "Historical":
            query.replace("from", "from_unixtime(%s, 'yyyy') as %s from".format(filter_col))
            final_query = query + "where" + "from_unixtime(%s,'yyyy')".format(filter_col) + "=" + business_date
            print("Final Query: %s".format(final_query))
        elif load_type == "Incremental":
            query.replace("from", "from_unixtime(%s, 'yyyy-mm-dd') as %s from".format(filter_col))
            final_query = query + "where" + "from_unixtime(%s,'yyyy-mm-dd')".format(filter_col) + "=" + business_date
            print("Final Query: %s".format(final_query))
        if len(final_query) != 0:
            final_df = spark.sql(final_query)
            return final_df
        else:
            print("Error: Wrong load type argument given")
            exit(1)
    except Exception as e:
        print(e)
        sys.exit(1)


def write_hive_table(final_df, db_name, table_name):
    target_table = db_name+"."+table_name
    final_df.write.format("parquet").mode("overwrite").insertInto(target_table)
