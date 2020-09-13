import sys
import os
import requests
import base64
import json
import logging
import pymysql
import time
import csv
from datetime import datetime
import pandas as pd
import jsonpath
import boto3



def main():
    try:
        conn = pymysql.connect(host, user=user, passwd=password, db=database, port=port, use_unicode = True, charset='utf8')
        cursor = conn.cursor()
    except:
        logging.error('could not connect to rds')
        sys.exit(1)

    cursor.execute("SELECT * FROM artists")
    colnames = [d[0] for d in cursor.description]
    artists = [dict(zip(colnames, row)) for row in cursor.fetchall()]
    artists = pd.DataFrame(artists)

    artists.to_parquet('artists.parquet', engine='pyarrow', compression='snappy')

    dt = datetime.utcnow().strftime("%Y-%m-%d")

    s3 = boto3.resource('s3')
    object = s3.Object('spoitfy-artists-hoony813', 'artists/dt={}/artists.parquet'.format(dt))
    data = open('artists.parquet', 'rb')
    object.put(Body=data)

if __name__ == "__main__":
    main()