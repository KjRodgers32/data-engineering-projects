import config # create config file for your postgres connection string
import subprocess
import os
import logging

import pandas as pd

from sqlalchemy import create_engine

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


logging.info('Starting file download...')
result = subprocess.run(["curl", "https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv", "-o", "taxi_zone_lookup.csv"])

if result.returncode != 0:
    logging.error("Error downloading file!")
    raise Exception("Download failed! Check the url!")

df = pd.read_csv('taxi_zone_lookup.csv')
logging.info('Download successful!')


logging.info('Starting file upload...')
engine = create_engine(config.PG_CONNECTION_STRING) # create postgresql engine

try:
    df.to_sql(con=engine, name='taxi_zone_lookup', if_exists='replace', index=False)
    logging.info('Upload successful!')
except Exception as e:
    logging.error(e)
    logging.error('Upload failed!')
finally:
    engine.dispose() # dispose of engine to free up resources

logging.info('Removing file...')
os.remove('taxi_zone_lookup.csv')
logging.info('Removed file successfully!')

