import os
import numpy as np
import pymysql
import pandas as pd
from definitions_of_const import datasets
import calendar

# In this code I always iterate through the various kinds of vehicles for each of the months there are tables for.
# I create here an array called 'instances' where I store the number of counts for each hour of the day (the array is initially defined as 24 floats 
# equal to 0.0). Then it adds up the 
# count from the query to the value in the position of the array corresponding to the hour of the day. 

#con = pymysql.connect(database="DatabaseName", user="user", password="password", host="host")

con = pymysql.connect(database="dbTest", user="francesco", password="fra_tra_pass", host="localhost")

for j, name_piece in enumerate (datasets):

    cur = con.cursor()
    cur.execute('select table_name from information_schema.tables where table_name like \'' + name_piece + '%\';')
    names = cur.fetchall()

    instances = []
    for n in range (0,24):
        instances.append(0.0)

    for i, table in enumerate (names):
        sql_query_busiest_hours = 'SELECT DATE_FORMAT(pickup_datetime, \'%H\') as hour_pickup, count(*) as Counts from `' 
        sql_query_busiest_hours += table[0] + '` group by DATE_FORMAT(pickup_datetime, \'%H\') order by Counts desc;'  
        cur.execute(sql_query_busiest_hours)
        busiest_hours = cur.fetchall()
### busiest_hours is an element such as: busiest_hours = (('10',45559),('09',293483),('03',38664),...  where there are 
### as many elements as up to 23, for every hour of the day.
        
        
        for m, hour in enumerate (busiest_hours):
            #### hour[0] is the element corresponding to the hour of the day, while hour[1] is the number of counts for that hour of day
            instances[int(hour[0])] += hour[1]
        
### In this next lines the hour with the highest counts is extracted as a value and then the counts for that hour is lower to 0,
### so that there is no inconsistency given from the pop() method that change the index of the hour

    first_busiest_hour_overall = instances.index(max(instances))
    instances[first_busiest_hour_overall] -= max(instances)
    second_busiest_hour_overall = instances.index(max(instances))
    instances[second_busiest_hour_overall] -= max(instances)
    third_busiest_hour_overall = instances.index(max(instances))
    instances[third_busiest_hour_overall] -= max(instances)

    print('The busiest hours for ' + name_piece + ' taxis are: ' + str(first_busiest_hour_overall) + ' ' + str(second_busiest_hour_overall)+ ' ' + str(third_busiest_hour_overall))
    print('')

con.close()

