import argparse
import json
import sys
from spark_session import spark_session
from execute import get_timestamp_epoch, query_hbase_table, write_hive_table
from util import Logging


def main():
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument('-s', '--database', dest='database', action='store', default=None)
        parser.add_argument('-t', '--table', dest='table_name', action='store', default=None)
        parser.add_argument('-j', '--config', dest='config', action='store', default=None)
        parser.add_argument('-d', '--business_date', dest="business_date", action='store', default=None)
        parser.add_argument('-k', '--kind', dest="load_type", action='store', default=None)
        args = parser.parse_args()
        db_name = args.database
        table_name = args.table_name
        load_type = None
        business_date = args.business_date
        if args.load_type.upper() == "H":
            load_type = 'Historical'
        elif args.load_type.upper() == "I":
            load_type = 'Incremental'
        spark = spark_session(table_name, load_type, business_date)
        logger = Logging.Log4j(spark)
        logger.info("ETL job started of {} for {}".format(table_name, business_date))
        business_date_epoch = get_timestamp_epoch(load_type, business_date)
        start_date_epoch = business_date_epoch[0]
        end_date_epoch = business_date_epoch[1]
        logger.info("Data scan range in epoch: {} and {}".format(start_date_epoch, end_date_epoch))
        logger.info("Get Table Config for: {}".format(table_name))
        table_config = json.load(open(args.config))
        schema = table_config[table_name]['Schema']
        filter_col = table_config[table_name]['FilterCol']
        query = table_config[table_name]['Query']
        logger.info("Schema: {}".format(schema))
        logger.info("Filter column: {}".format(filter_col))
        logger.info("Query: {}".format(query))
        final_df = query_hbase_table(spark, schema, table_name, logger, query, filter_col, start_date_epoch,
                                     end_date_epoch)
        final_df.show()
        write_hive_table(logger, final_df, db_name, table_name)
    except Exception as e:
        print(e)
        sys.exit(1)


if __name__ == '__main__':
    main()
