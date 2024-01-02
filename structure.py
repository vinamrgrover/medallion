"""
Script for creating structure of the tables in DB
"""

import sqlalchemy
from dotenv import load_dotenv
import os
import pandas as pd
import glob

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")


if __name__ == '__main__':
    engine = sqlalchemy.create_engine(
        f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )
    conn = engine.connect()

    file_paths = glob.glob('data/*.txt')

    for file in file_paths:
        df = pd.read_csv(file, delimiter = '|', header = 0)

        table_name = file.replace('.txt', '') \
                         .replace('data/', '')


        df = df.head(0)
        df.to_sql(con = engine, name = table_name, if_exists = 'replace', index = False)

    conn.close()
