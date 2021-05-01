# Process” the file entails to parse it and store into the database,
# making sure you do so based on the filename and the event_code
#

import os
import fnmatch
import csv
import pymysql
import pandas as pd
from sqlalchemy import create_engine
import mysql.connector as mysql
from mysql.connector import Error
import seaborn as sb


files = os.listdir(
    'C:/Users/asumo/Desktop/ALTAPAY_ASSIGNMENT/technical-test-data/technical-test-data/data/')

print(files)
data = ''

for file in files:
    if fnmatch.fnmatch(file, '82242267*0000.csv'):
        data = file
        print("File Name :", data)


df = pd.read_csv(
    'C:/Users/asumo/Desktop/ALTAPAY_ASSIGNMENT/technical-test-data/technical-test-data/data/'+data, sep=';')

# Data cleaning and indexing
df = df.replace(to_replace=r'\\', value='', regex=True)
header_row = 1
df.columns = df.iloc[header_row]
df = df.drop(header_row)
df = df.reset_index(drop=True)
df = df.drop(0)
df = df.reset_index(drop=True)
del df["refund_id"]
df1 = df.dropna(axis='columns')
df1 = df.set_index('capture_id')
print(df1.columns)
# print(df1.head(5))


tableName = 'datatable'
dataFrame = df1


# This engine just used to query for list of databases
sqlEngine = create_engine('mysql+pymysql://{user}:{pw}@{host}:{port}'.format(
    user="root", pw="password", host="localhost", port=3306))

# Query for existing databases
sqlEngine.execute("CREATE DATABASE IF NOT EXISTS CSV_DATABASE")

# Go ahead and use this engine

sqlEngine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                          .format(user="root",
                                  pw="password",
                                  db="CSV_DATABASE"))

# select the database
sqlEngine.execute("USE CSV_DATABASE")


# Insert whole DataFrame into MySQL

df2 = df1.to_sql('datatable', con=sqlEngine,
                 if_exists='append', index=True, chunksize=1000)
print(df2)


# Query from the databse using engine

with sqlEngine.connect() as con:

    rs = con.execute(
        "select * from datatable where type='sale'")

    for row in rs:
        print(row)

    rs1 = con.execute(
        'select count(*) from datatable where event_code=106')
    for row in rs1:
        print('Total Row : ', row)
    rs2 = con.execute(
        "select * from datatable where capture_id='14f05891d85a'")
    for row in rs2:
        print(row)
