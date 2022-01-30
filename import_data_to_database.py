import numpy as np
import datetime as dt
from dateutil.relativedelta import relativedelta
import pandas as pd
from sqlalchemy import create_engine
import pandavro as pdavro
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from data_import_suppl_func import *

## I defined some arrays containing strings that since are used for other script I wrote in a different script: definition_of_const.py

from definition_of_const import *

## definition of the function that imports the data and writes them if the file is not present

def import_and_write_function(name_tables, date):
    for i, item in enumerate (name_tables):
    url = item + date.strftime("%Y-%m") + ".csv"
    name_of_table = name_tables[i] + '_' + date.strftime("%Y-%m")
## Here the code checks if there is already a csv file in the working directory

### In case there is already the functions called are just the ones to infer the types and to create the database from the  
    check_file_existance = check_csv_existance()
    if (check_file_existance == True):
        data = pd.DataFrame()
        for chunk in pd.read_csv('path_to_file' + name_of_table, chunksize=300000):
            data = pd.concat([data, chunk], ignore_index=True)
         types_table = infer_types(dataframe= data, types_array_element= types_table_alter[i])
         create_and_fill_dbTable(data, name_of_table, types_table)
    else:
        data = pd.DataFrame()
        for chunk in pd.read_csv(url, chunksize=300000):
        data = pd.concat([data, chunk], ignore_index=True)

#### After having imported the data into a DataFrame the code extracts the columns names and types and add them to the elements of the 
#### types_table_alter following the sintax of MySQL, since these will be used to execute the CREATE queries through this same code.

        types_table = infer_types(dataframe = data, types_array_element = types_table_alter[i])
            
### Connection to the database and creation of the table. name_of_table will be similar to 'Yellow_Taxi_2021-01' so that inside the 
### SQLQueries files will be possible to iterate over all the tables for yellow taxis.

        create_and_fill_dbTable(data, name_of_table, types_table)

### If one decides to, here are colled the functions to write the data into parquet and csv files

        write_to_csv(data, folder_path='/path/to/folder', name_of_table, check_file_existance)

#### From here to the end the code reiterates the same process for data coreesponding to the datetime.date defined in the elif condition. These condition 
#### where defined looking at the data: in 2013 green taxis's data were added to the yellow ones. In 2015 the for-hire vehicles were added. 
#### In february 2019 the high-volume vehicles are added. Also in january 2019 the data of for-hire vehicle were found at a different URL.
#### for this reason after that date the third element of the 'datasets_url' array is being redefined.

###  Here starts the main body of the code: current date was defined at the start equal the the start date, and at the end of one loop 
###  it is increased by one month with the 'relativedelta' method

while current_date <= end_date:
#### In this case I decided to let pandas infer the types, so at every start of a loop I define an "empty" array where I add the types and 
#### names of the columns as stings

    types_table_alter = [" (", " (", " (", " ("]       
    if (current_date < dt.date(2013,1,1)):
### With the slicing of the array datasets_url I can be sure that, even if before 2013 only yellow taxis' data were registered,
### the code doesn't look for URLs of green taxis and the others. If in the future new kinds of vehicle will be surveyed these can be added to the array
        import_and_write_function(datasets_url[0:1], current_date)
        

#### From here to the end the code reiterates the same process for data coreesponding to the datetime.date defined in the elif condition. These condition 
#### where defined looking at the data: in 2013 green taxis's data were added to the yellow ones. In 2015 the for-hire vehicles were added. 
#### In february 2019 the high-volume vehicles are added. Also in january 2019 the data of for-hire vehicle were found at a different URL.
#### for this reason after that date the third element of the 'datasets_url' array is being redefined.
  

    elif (current_date < dt.date(2015,8,1) and current_date >= dt.date(2013,1,1)):
        import_and_write_function(datasets_url[0:2], current_date)
        
                
    elif (current_date < dt.date(2019,1,1) and current_date >= dt.date(2015,8,1)):
        import_and_write_function(datasets_url[0:3], current_date)
        
        
    elif (current_date < dt.date(2019,2,1) and current_date >= dt.date(2019,1,1)):
        datasets_url[2] = "https://nyc-tlc.s3.amazonaws.com/trip+data/fhv_tripdata_"   ## Redefinition of the URL for the for-hire vehicles as anticipated
        import_and_write_function(datasets_url[0:3], current_date)
        
    else:
        import_and_write_function(datasets_url[0:1], current_date)
        
    
    current_date += relativedelta(months=1)
    
   



