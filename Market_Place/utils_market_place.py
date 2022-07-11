import requests
import pandas as pd
from snowflake.sqlalchemy import URL
import urllib.request, json
import snowflake.connector
from pandas.io import sql
import snowflake.connector
from sqlalchemy import create_engine
import io
from math import *
import numpy as np
import csv
import gzip
from io import BytesIO, StringIO
from zipfile import ZipFile
import os




## return info from ini file
def get_info_from_ini_file(config):
    schema_name = config["Data"]["schema"]

    urls = config["Data"]["urls"]
    urls = urls.replace(" ", "")
    urls = urls.split(",")

    table_name = config["Data"]["table"]

    script_creation = config["Data"]["creation_table"]

    separateur = config["Data"]["separateur"]
    separateur = separateur.replace(" ", "")


    chunk = config["Data"]["chunksize"]
    chunk = int(chunk)

    name_columns = config["Data"]["name_columns"]
    name_columns = name_columns.split(",")

    file_type = config["Data"]["type"]
    file_type = file_type.split(",")


    row_to_skip = config["Data"]["skip"]
    row_to_skip = int(row_to_skip)


    enco = config["Data"]["enco"]

    return schema_name, urls, table_name, script_creation, separateur, chunk, name_columns, file_type, row_to_skip, enco

## get informaion in the ini file
def get_informations_SF(config):
    account = config["Snowflake"]["account"]
    user = config["Snowflake"]["user"]
    password = config["Snowflake"]["password"]
    warehouse = config["Snowflake"]["warehouse"]
    return account,user,password,warehouse




## get data from url with request
def get_data(url):
    r = requests.get(url, stream=True)
    return r


## decompose and save a csv file from a zip
def decompose_save_csv_zip(mlz,file,directory,separateur,table_name,nb,chunk,name_columns,row_to_skip,enco):
    df_chunk = pd.read_csv(mlz.open(file), chunksize=chunk ,sep=separateur,low_memory=False,names=name_columns,encoding=enco,skiprows=row_to_skip)
    for df in df_chunk :
        df.columns = name_columns
        df.to_csv(directory +  table_name + str(nb) + ".csv", index = False,line_terminator='\n',sep=';')
        nb+=1

    return nb



## decompose and save a csv file from a gz
def handle_csv_gz(data,directory,separateur,table_name,nb,chunk,name_columns,row_to_skip,enco):
    try:
        if (int(data.headers['Content-length']) <= 21 ):
            return nb
    
    except KeyError:
        print("impossible to know the size possible empty file")
    data = gzip.GzipFile(fileobj=io.BytesIO(data.raw.read()))
    df_chunk = pd.read_csv(data,chunksize = chunk,sep=separateur,engine='python',names=name_columns,skiprows=row_to_skip,encoding=enco)
    for df in df_chunk:
        if (len(df) > 0):
            df.columns = name_columns
            df.to_csv(directory +  table_name + str(nb) + ".csv", index = False,line_terminator='\n',sep=';')
            nb += 1
    return nb

## decompose and save a csv file
def handle_csv(data,directory,separateur,table_name,nb,chunk,name_columns,row_to_skip,enco):
    df_chunk = pd.read_csv(io.BytesIO(data.content),chunksize = chunk,sep=separateur,engine='python',names=name_columns,skiprows=row_to_skip,encoding=enco)
    for df in df_chunk:
        if (len(df) > 0):
            df.columns = name_columns
            df.to_csv(directory +  table_name + str(nb) + ".csv", index = False,line_terminator='\n',sep=';')
            nb += 1
    return nb



## with file type call the accurate function
def handle_decompose_save(data,url,directory,separateur,table_name,nb,chunk,name_columns,file_type,row_to_skip,enco,config):
    if (len(file_type) == 2):
        if (file_type[0] == "gz" and file_type[1] == "csv"):
            
            nb = handle_csv_gz(data,directory,separateur,table_name,nb,chunk,name_columns,row_to_skip,enco)
            return nb, "csv"


        elif (file_type[0] == "zip" and file_type[1] == "csv"):

            files_to_get = config["Data"]["zip_file_to_get"]
            files_to_get = files_to_get.split(",")

            
            mlz = ZipFile(io.BytesIO(data.raw.read()))
            files = mlz.namelist()
            for file in files:
                    for file_to_get in files_to_get:
                        if (file.find(file_to_get) != -1):
                            nb = decompose_save_csv_zip(mlz,file,directory,separateur,table_name,nb,chunk,name_columns,row_to_skip,enco)
                
            return nb, "csv"
    else:

        if (file_type[0] == "csv"):

            nb = handle_csv(data,directory,separateur,table_name,nb,chunk,name_columns,row_to_skip,enco)

            return nb, "csv"
        
    

## create sf connexion
def create_connexion_snowflake(account,user,password,warehouse):
    ctx = snowflake.connector.connect(
    user= user,
    password= password,
    account=account,
    warehouse=warehouse,
    )
    cursnow = ctx.cursor()
    return cursnow, ctx

## create objetcts in sf
def create_objects_sf(cursor,schema_name,script_creation,var_type,table_name):
    cursor.execute("create database IF NOT EXISTS REF_MARKETPLACE;")
    cursor.execute("use database REF_MARKETPLACE;")

    cursor.execute("create schema IF NOT EXISTS " + schema_name + " ;")
    cursor.execute("use schema " + schema_name + " ;")

    cursor.execute(script_creation)

    if (var_type == "csv"):
        creation_ff = "create or replace file format  CSV type = 'CSV'   SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = 'NONE' FIELD_DELIMITER = ';' RECORD_DELIMITER = '\n' error_on_column_count_mismatch=false NULL_IF = ('NULL') ;"
        cursor.execute(creation_ff)
    elif (var_type == "json"):
        creation_ff = " "
        cursor.execute(creation_ff)

## load and insert data into tabel sf
def load_into_table(cursor, table_name, schema_name,var_type, directory,wh):
   
    os.system('snowsql  --config test.config -c my_connection -d REF_MARKETPLACE -s '+ schema_name +' -q "PUT file:///' + directory +'/* @%' + table_name + ';"')

    cursor.execute("truncate table " + table_name +" ;")

    if var_type == "csv":
        cursor.execute("copy into " + table_name +" file_format = CSV ;")
    
    cursor.execute("remove @%"+ table_name +" ;")
    cursor.execute("alter warehouse " + wh + " suspend;")







