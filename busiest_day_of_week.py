import numpy as np
import pymysql
from definitions_of_const import datasets
import calendar


#con = pymysql.connect(database="DatabaseName", user="user", password="password", host="host")

con = pymysql.connect(database="dbTest", user="francesco", password="fra_tra_pass", host="localhost")


for j, name_piece in enumerate (datasets[0:2]):

    cur = con.cursor()
    cur.execute('select table_name from information_schema.tables where table_name like \'' + name_piece + '%\';')
    names = cur.fetchall()

    low_single_trip_overall = []

    for i, table in enumerate (names):

        sql_query_lowest_single_trip_day = 'SELECT DAYOFWEEK(pickup_datetime), COUNT(*) AS Cnt from `' + table[0] + '` '
        sql_query_lowest_single_trip_day += 'WHERE passenger_count = 1 GROUP BY DAYOFWEEK(pickup_datetime) ORDER BY Cnt ASC;'
        cur.execute(sql_query_lowest_single_trip_day)
        lowest_single_trip_day = cur.fetchall()

### lowest_single_trip_day is an element such as: ((1,),(2,),(4,),(6,),(3,),(5,),(7,))
        
        low_single_trip_overall.append(lowest_single_trip_day[0][0])



    single_trip_day = np.argmax(np.bincount(low_single_trip_overall))


    print('The busiest day for ' + name_piece + ' taxis is: ' + calendar.day_name[single_trip_day-1])
    print('')

con.close()
