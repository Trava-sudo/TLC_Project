import pymysql
import datetime as dt
from dateutil.relativedelta import relativedelta
import pyspark 
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from sqlalchemy import create_engine
from definitions_of_const import datasets  ### Import from the MobiSQL script to be sure one is using the same names and in case one changes 
                                          ### the name of created tables the script works on the right tables

col_time_label = ['tpep_', 'lpep_', '', '']  

con = pymysql.connect(database="DatabaseName", user="user", password="password", host="host")

for j, name_piece in enumerate (datasets):

    cur = con.cursor()
    cur.execute('select table_name from information_schema.tables where table_name like \'' + name_piece + '%\';')
    names = cur.fetchall()

## Here the col_time_label being defined as it is allows to rename all the right columns within a single for cycle
## tpep_pickup_time, lpep_pickup_time, pickup_time, pickup_time are the names respectively for Yellow, Green, For Hire and High Volume vehicles. The same fo dropoff.

    for i, table in enumerate (names):
        cur.execute('ALTER TABLE `' + table[0] + '` MODIFY ' + col_time_label[j] + 'pickup_datetime DATETIME;')
        cur.execute('ALTER TABLE `' + table[0] + '` RENAME COLUMN ' + col_time_label[j] + 'pickup_datetime TO pickup_datetime;')
        cur.execute('ALTER TABLE `' + table[0] + '` MODIFY ' + col_time_label[j] + 'dropoff_datetime DATETIME;')
        cur.execute('ALTER TABLE `' + table[0] + '` RENAME COLUMN ' + col_time_label[j] + 'dropoff_datetime TO dropoff_datetime;')


con.close()
