import os
import numpy as np
import pymysql
import pandas as pd
from definition_of_const import datasets


con = pymysql.connect(database="DatabaseName", user="user", password="password", host="host")

for j, name_piece in enumerate (datasets):

    cur = con.cursor()
    cur.execute('select table_name from information_schema.tables where table_name like \'' + name_piece + '%\';')
    names = cur.fetchall()

    trip_distance_tot = 0.0
    travelled_time_tot = 0.0

    for i, table in enumerate (names):

        sql_query_trip_distance = 'SELECT'
        sql_query_trip_distance += ' SUM(trip_distance) FROM `' + table[0] + '`;'
        cur.execute(sql_query_trip_distance)
        dist_part = cur.fetchall()

        sql_query_total_hour = 'SELECT' 
        sql_query_total_hour += ' SUM(timediff(dropoff_datetime, pickup_datetime)) FROM `' + table[0] + '`;'
        cur.execute(sql_query_total_hour)

        travelled_time = cur.fetchall()
        travelled_time_float = float(travelled_time[0][0])
        trip_distance_tot += dist_part[0][0]
        travelled_time_tot += (travelled_time_float/3600.0)
        average_dit_per_hour = trip_distance_tot/travelled_time_tot

    print('The total trip distance travelled by ' + name_piece + ' taxis is: ' + str(trip_distance_tot))
    print('')
    print('The total travelled time by ' + name_piece + ' taxis is: ' + str(travelled_time_tot))
    print('')
    print('The average of travelled distance per hour for ' + name_piece + ' taxis is: ' + str(average_dit_per_hour))
    print('')

    #cur.execute('SELECT SUM(trip_distance) FROM ' + str(names[i][0]) 

con.close()
