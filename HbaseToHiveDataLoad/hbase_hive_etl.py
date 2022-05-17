import argparse
import json
import sys
from util.spark_session import spark_session, get_timestamp_epoch
from job.execute import query_hbase_table, write_hive_table


def main():
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument('-s', '--database', dest='database', action='store', default=None)
        parser.add_argument('-t', '--table', dest='table_name', action='store', default=None)
        parser.add_argument('-c', '--config', dest='config', action='store', default=None)
        parser.add_argument('-d', '--business_date', dest="business_date", action='store', default=None)
        parser.add_argument('-k', '--kind', dest="load_type", action='store', default=None)
        args = parser.parse_args()
        db_name = args.database
        table_name = args.table_name
        load_type = None
        business_date = args.business_date
        if args.type.upper() == "H":
            load_type = 'Historical'
        elif args.type.upper() == "I":
            load_type = 'Incremental'
        spark = spark_session(table_name, load_type, business_date)
        business_date_epoch = get_timestamp_epoch(load_type, business_date)
        table_config = json.load(open(args.config))
        schema = table_config[table_name]['Schema']
        filter_col = table_config[table_name]['FilterCol']
        query = table_config[table_name]['Query']
        final_df = query_hbase_table(spark, schema, table_name, load_type, query, filter_col, business_date_epoch)
        final_df.show()
        write_hive_table(final_df, db_name, table_name)
    except Exception as e:
        print(e)
        sys.exit(1)


if __name__ == '__main__':
    main()
