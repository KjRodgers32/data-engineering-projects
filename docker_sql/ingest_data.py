import os
import psycopg2
import logging
import argparse

import pandas as pd
import numpy as np
import pyarrow.parquet as pq
import pyarrow as pa

from sqlalchemy import create_engine
from time import time

## Optional send logs to a file
# from datetime import date
# today = date.today().strftime("%Y_%m_%d")
# log_file = f'ingest_data_logs_{today}.txt'
# file_handler = logging.FileHandler
# file_handler.setLevel(logging.INFO)
# file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


parquet_file_name = 'yellow_tripdata_202109.parquet' # output name for parquet file

def main(params):
    try:
        # define variables from command line parameters
        user = params.user
        password = params.password
        host = params.host
        port = params.port
        db = params.db
        table_name = params.table_name
        url = params.url

        logging.info('Starting file download...')
        os.system(f'curl {url} -o {parquet_file_name}')
        logging.info('Finished file donwload!')

        chunk_size = 100_000
        parquet_file = pq.ParquetFile(parquet_file_name)


        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}') # create postgresql engine

        # only grabbing the first row of parquet to establish the schema for the table
        define_schema = next(parquet_file.iter_batches(batch_size=1))
        define_schema_df = pa.Table.from_batches([define_schema]).to_pandas()

        command = pd.io.sql.get_schema(define_schema_df, name=table_name, con=engine)

        conn = psycopg2.connect(f'postgresql://{user}:{password}@{host}:{port}/{db}')
        cur = conn.cursor()

        try:
            cur.execute(command) # create table command

            cur.close()
            conn.commit()

            logging.info('Writing files to table...')
            for batch in parquet_file.iter_batches(batch_size = chunk_size):
                t_start = time()

                df = batch.to_pandas()
                df.to_sql(con=engine, name=table_name, if_exists='append', index=False)

                t_end = time()

                logging.info('Inserted chunk into table..., this took %.3f second(s)' %(t_end - t_start))
        # close database connections even if error occurs
        finally:
            conn.close()
            cur.close()

        logging.info('Finished writing files to table!')

    except psycopg2.OperationalError as e:
        logging.error(f'Failed to connect to database: {e}')

    except Exception as e:
        logging.error(f'Failed during data ingestiong: {e}')


if __name__ == '__main__':
    # argument for user, pass, host, port, database name, table name, url of parquet
    parser = argparse.ArgumentParser(description='Ingest parquet data to PostgreSQL database')

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='user pass for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='table name for postgres')
    parser.add_argument('--url', help='url of parquet file')

    args = parser.parse_args()

    logging.info(args)

    main(args)

    logging.info('Ingestion finished successfully')

    logging.info('Removing parquet files...')
    os.remove(parquet_file_name) # remove parquet file
    logging.info('Removed parquet files successfully')