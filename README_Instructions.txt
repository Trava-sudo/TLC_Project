"import_data_to_database.py":

This code is used to import data from the TLC website directly into pandas' DataFrame, and then create databases with MySQL and fill them with said data.

The description of the single commands is done within the file. 

Broadly describing the functioning of the code:

The user can choose a start_date from which they want to take the data. 
In the code are defined arrays containing the URLs of the data set, since they are all the same for all files, and differ from each other only by the file name:

i.e.  https://s3.amazonaws.com/nyc-tlc/trip+data/ is the URL in common for both yellow_tripdata_2021-01.csv  and green_tripdata_2021-01.csv (more in general 'yellow_***.csv' and green_***.csv')

In a while loop over a 'current_date' variable, which is increased after everylopp by one month, there is the main body of the code.

In here through the pandas.read_csv method the data are imported into a DataFrame, in chunks to overcome performance issues.

If is needed there is the possibility to call the function write_to_parquet or write_to_csv. 

I made the choice to not define the types of the data myself, but let pandas determine those. This because for different time periods the data can have a different number of columns, and also data for Yellow Taxis differ from data of For Hire vehicle.
After having imported the data the code then connects to a Database previously created, and creates the table using the data types inferred from pandas, with the necessary change of syntax (i.e. float64 -> float or string -> VARCHAR(255) )

In this way the user doesn't have to checck all the datatypes of the past and can also suppose that, even if future data will differ in columns, the code will be able to assign the right types to the SQL command that creates the corresponding table.

The to_sql() method is also used with the chunksize option. 

This is repeated for every month (while loop) and for all the types of data registered (through a for cycle over an array containing the URLs of yellow taxis, green taxis, for hire and high volume for hire vehicles). Through the use of the slicing of arrays one can be sure that for times where the newest type of vehicle (in desceding order high volume, for hire and green taxis) the code just look for the url corresponding to the kinds of data that were already being registered at that time. This allows to add new kinds of vehicle the the array and use the slicing to mantain the consistency.



"Average_dist_per_hour.py":

This code is used to calculated the average distance travelled per hour by yellow and green taxis. This is done with a for cycle since there are many tables for the yeelow and green taxis as there are months in the time period chosen when importing data.
The for cycle takes advantage of the possibilty of databases to iterate a query over tables (and columns) with similar part of names. In this way the code takes the SUM(trip_distance) and the SUM(timediff(dropoff_datetime, pickup_datetime)) and adds them to a float variable, and doing so for every month at the end of the running time the output is the average travelled distance for both kind of taxis. This could be extended to the other vehicles: the user just needs to cheange the name of the pickup and dropoff time columns with 'ALTER TABLE ..... REPLACE NAME .... TO pickup_time (dropoff)'
This has been done to the yellow and green taxis' table with 'Query_alter_time_col.py' since there is the problem of iterating through all the table of different months.










