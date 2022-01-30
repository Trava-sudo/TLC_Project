from sqlalchemy import create_engine   
import os 
from pyspark.sql import SparkSession



    
            
def infer_types(dataframe, types_array_element):    
    col_labels = dataframe.columns            
    col_types = dataframe.dtypes
    for j, label in enumerate (col_labels):
        if ((col_types[j] == 'float64')==False) and ((col_types[j] == 'int64')==False):
            if ("time" in label):
                types_array_element += label + " DATETIME,"
            else:
                types_array_element += label + " VARCHAR(255),"
        else:
            types_array_element += str(label) + " float,"
    types_element = types_array_element[:-1] + ")"
    return types_element

def create_and_fill_dbTable(dataframe, table_name, table_types):
    engine = create_engine('mysql+pymysql://user:password@host/Database_Name')
    engineconn = engine.connect()
    engineconn.execute('CREATE TABLE IF NOT EXISTS `' + table_name + '`' + table_types + ';')
    dataframe.to_sql(name = table_name, con = engine, if_exists = 'replace', index = False, chunksize = 80000)
    engine.dispose()

def write_to_parquet(dataframe, folder_path, table_name, condition):
    if(condition==False):
        dataframe.to_parquet(folder_path + table_name +'.parquet', index = False)

def check_parquet_existance(path_to_file, table_name):
    if(not os.path.exists(path_to_file + table_name + ".csv")):
        return False
    if(os.path.exists(path_to_file + table_name + ".csv")):
        return True
        
def check_avro_existance(path_to_file, table_name):
    if(not os.path.exists(path_to_file + table_name + ".csv")):
        return False
    if(os.path.exists(path_to_file + table_name + ".csv")):
        return True
        
        
def check_csv_existance(path_to_file, table_name):
    if(not os.path.exists(path_to_file + table_name + ".csv")):
        return False
    if(os.path.exists(path_to_file + table_name + ".csv")):
        return True
 
    
def write_to_csv(dataframe, folder_path, table_name, condition):
    if(condition==False):
        col_labels_csv = dataframe.types
        dataframe.to_csv(folder_path + table_name + ".csv", header=col_labels_csv, index = False, chunksize= 30000)

def from_parquet_to_avro(file, folder_directory, condition):
    if(condition==False):
        spark = SparkSession.builder.getOrCreate()
        df = spark.read.parquet(file + '.parquet')
        df.write.format("com.databricks.spark.avro").save(folder_directory + '/' + file)

        
    







