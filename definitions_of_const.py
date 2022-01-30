## I use this script to define names of tables and URLs so that when they are used in different script they maintain consistency 
## (i.e. changing the name of table, extracted from the 'datasets array, would mean I need to chenge it in all the other scripts that 
## connect to the SQL database to extract data and execute queries)

import datetime as dt

### Definition of start and end date. For the end date it is used the datetime.date.today() 
### method so that everytime one runs the code it is sure the get the latest data up to that same day.

start_date = dt.date(2021, 1, 1)
end_date = dt.date.today()

current_date = start_date   


## These string will be used for the name of the tables created in the Database, iterating through the array as the for loop 
## iterates over the "datasets_url" array.
datasets = ["Yellow_Taxi", "Green_Taxi", "For_Hire", "High_Volume"]  

datasets_url = ["https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_", "https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_", "https://s3.amazonaws.com/nyc-tlc/trip+data/fhv_tripdata_", "https://nyc-tlc.s3.amazonaws.com/trip+data/fhvhv_tripdata_"]

#### In these next commented lines are defined the types of the data in case the user wanted to input them when creating the tble in the database.
#### One can add to these the datatypes for previous or future timeperiods, where the amount and type of data differs:
#### in this case one would iterate not only through the different kind of vehicles (i.e. yellow, green, ....)
#### but also trough the different times. Lines 37-40 would become arrays themselves containing long strings for the data types 
#### of tables in different times

#types_yellow_table = " (VendorID float,tpep_pickup_datetime DATE,tpep_dropoff_datetime DATE,passenger_count float,trip_distance float,RatecodeID float,store_and_fwd_flag VARCHAR(255),PULocationID float,DOLocationID float,payment_type float,fare_amount float,extra float,mta_tax float,tip_amount float,tolls_amount float,improvement_surcharge float,total_amount float,congestion_surcharge float);"
#types_green_table = " (VendorID float,lpep_pickup_datetime DATE,lpep_dropoff_datetime DATE,store_and_fwd_flag VARCHAR(255),RatecodeID float,PULocationID float,DOLocationID float,passenger_count float,trip_distance float,fare_amount float,extra float,mta_tax float,tip_amount float,tolls_amount float,ehail_fee float,improvement_surcharge float,total_amount float,payment_type float,trip_type float,congestion_surcharge float);"
#types_for_hire_table = " (dispatching_base_num VARCHAR(255),pickup_datetime DATE,dropoff_datetime DATE,PULocationID float,DOLocationID float,SR_Flag float,Affiliated_base_number VARCHAR(255));"
#types_high_volume_table = " (hvfhs_license_num VARCHAR(255),dispatching_base_num VARCHAR(255),pickup_datetime DATE,dropoff_datetime DATE,PULocationID float,DOLocationID float,SR_Flag float);"

#types_table = [types_yellow_table, types_green_table, types_for_hire_table, types_high_volume_table]

#### Same as above, the next dtypes_* are used when importing the data into a DataFrame, if the user prefers to define the types 
#### insted of letting pandas infer them-

#dtypes_yellow_table = {"VendorID": float, "tpep_pickup_datetime": "string", "tpep_dropoff_datetime": "string", "passenger_count": float, "trip_distance": float, "RatecodeID": float, "store_and_fwd_flag": "string", "PULocationID": float, "DOLocationID": float, "payment_type": float, "fare_amount": float, "extra": float, "mta_tax": float,"tip_amount": float, "tolls_amount": float, "improvement_surcharge": float, "total_amount": float}
#dtypes_green_table = {"VendorID": float, "lpep_pickup_datetime": "string", "lpep_dropoff_datetime": "string", "store_and_fwd_flag": "string", "RatecodeID": float, "PULocationID": float, "DOLocationID": float, "passenger_count": float, "trip_distance": float, "fare_amount": float, "extra": float, "mta_tax": float}
#dtypes_for_hire_table = {"dispatching_base_num": "string", "pickup_datetime": "string", "dropoff_datetime": "string", "PULocationID": float, "DOLocationID": float, "SR_Flag": float, "Affiliated_base_number": "string"}
#dtypes_high_volume_table = {"hvfhs_license_num": "string", "dispatching_base_num": "string", "pickup_datetime": "string", "dropoff_datetime": "string", "PULocationID": float, "DOLocationID": float, "SR_Flag": float}

#dtypes_table = [dtypes_yellow_table, dtypes_green_table, dtypes_for_hire_table, dtypes_high_volume_table]
