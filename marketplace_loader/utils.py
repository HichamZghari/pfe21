import pandas as pd
import requests 
import io
import csv
#from google.cloud import storage
import snowflake.connector
import configparser
from zipfile import ZipFile
from sqlalchemy import create_engine
from math import *
import gzip
from io import BytesIO, StringIO
import os
from bs4 import BeautifulSoup, SoupStrainer

from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__


from concurrent.futures import ThreadPoolExecutor
import threading


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

    database = config["Data"]["database"]

    chunk = config["Data"]["chunksize"]
    chunk = int(chunk)


    name_columns = config["Data"]["name_columns"]
    name_columns = name_columns.split(",")

    file_type = config["Data"]["type"]
    file_type = file_type.split(",")


    row_to_skip = config["Data"]["skip"]
    row_to_skip = int(row_to_skip)


    enco = config["Data"]["enco"]

    return schema_name, urls, table_name, script_creation, separateur, chunk, name_columns, file_type, row_to_skip, enco, database


## get informaion in the ini file
def get_informations_SF(config):
    account = config["Snowflake"]["account"]
    user = config["Snowflake"]["user"]
    password = config["Snowflake"]["password"]
    warehouse = config["Snowflake"]["warehouse"]
    return account,user,password,warehouse


def get_data(url):
    r = requests.get(url, stream=True)
    return r

def handle_df_chunk(df,name_columns,name_bucket,schema_name,table_name,nb):
    df.columns = name_columns
    df = df.to_csv(sep = ';',index=False)
    
    connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    blob_client = blob_service_client.get_blob_client(container=name_bucket, blob=schema_name + '/' + table_name + str(nb) + ".csv")
    blob_client.upload_blob(df)
    
    #storage_client = storage.Client()
    #bucket = storage_client.get_bucket(name_bucket)
    #blob = bucket.blob(schema_name + '/' + table_name + str(nb) + ".csv")
    #blob.upload_from_string(data=df)




## decompose and save a csv file from a zip
def decompose_save_csv_zip(mlz,file,name_bucket,separateur,table_name,nb,chunk,name_columns,row_to_skip,enco,schema_name):
   
    df_chunk = pd.read_csv(mlz.open(file), chunksize=chunk ,sep=separateur,low_memory=False,names=name_columns,encoding=enco,skiprows=row_to_skip)
    for df in df_chunk :
        nb = nb + 1
        handle_df_chunk(df,name_columns,name_bucket,schema_name,table_name,nb)
        

    return nb



## decompose and save a csv file from a gz
def handle_csv_gz(data,name_bucket,separateur,table_name,nb,chunk,name_columns,row_to_skip,enco,schema_name):
    try:
        if (int(data.headers['Content-length']) <= 21 ):
            return nb
    
    except KeyError:
        print("impossible to know the size possible empty file")
    data = gzip.GzipFile(fileobj=io.BytesIO(data.raw.read()))

    df_chunk = pd.read_csv(data,chunksize = chunk,sep=separateur,engine='python',names=name_columns,skiprows=row_to_skip,encoding=enco)
    for df in df_chunk:
        if (len(df) > 0):
            nb = nb + 1
            handle_df_chunk(df,name_columns,name_bucket,schema_name,table_name,nb)
    return nb




## decompose and save a csv file
def handle_csv(data,name_bucket,separateur,table_name,nb,chunk,name_columns,row_to_skip,enco,schema_name):
    df_chunk = pd.read_csv(io.BytesIO(data.content),chunksize = chunk,sep=separateur,engine='python',names=name_columns,skiprows=row_to_skip,encoding=enco)
    for df in df_chunk:
        if (len(df) > 0):
            nb = nb + 1
            handle_df_chunk(df,name_columns,name_bucket,schema_name,table_name,nb)
    return nb



## decompose and save a txt file
def handle_txt(data,name_bucket,separateur,table_name,nb,chunk,name_columns,row_to_skip,enco,schema_name):
    df_chunk = pd.read_csv(io.BytesIO(data.content),chunksize = chunk,sep=separateur,engine='python',names=name_columns,skiprows=row_to_skip,encoding=enco)
    for df in df_chunk:
        if (len(df) > 0):
            nb = nb + 1
            handle_df_chunk(df,name_columns,name_bucket,schema_name,table_name,nb)
    return nb


## with file type call the accurate function
def handle_decompose_save(data,url,name_bucket,separateur,table_name,nb,chunk,name_columns,file_type,row_to_skip,enco,config,schema_name):
    #os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "marketplace.json"
    
    if (len(file_type) == 2):
        

        if (file_type[0] == "gz" and file_type[1] == "csv"):
            
            nb = handle_csv_gz(data,name_bucket,separateur,table_name,nb,chunk,name_columns,row_to_skip,enco,schema_name)
            return nb


        elif (file_type[0] == "zip" and file_type[1] == "csv"):

            files_to_get = config["Data"]["zip_file_to_get"]
            files_to_get = files_to_get.split(",")

            
            mlz = ZipFile(io.BytesIO(data.raw.read()))
            files = mlz.namelist()
            for file in files:
                    for file_to_get in files_to_get:
                        if (file.find(file_to_get) != -1):
                            nb = decompose_save_csv_zip(mlz,file,name_bucket,separateur,table_name,nb,chunk,name_columns,row_to_skip,enco,schema_name)
                
            return nb
            
    else:

        if (file_type[0] == "csv"):
            nb = handle_csv(data,name_bucket,separateur,table_name,nb,chunk,name_columns,row_to_skip,enco,schema_name)

            return nb

        elif (file_type[0] == "txt"):
            
            nb = handle_txt(data,name_bucket,separateur,table_name,nb,chunk,name_columns,row_to_skip,enco,schema_name)

            return nb


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
def create_objects_sf(cursor,schema_name,script_creation,var_type,table_name,database):
    cursor.execute("use role SYSADMIN;")
    cursor.execute("create database IF NOT EXISTS " + database + ";")
    cursor.execute("use database " + database + " ;")

    cursor.execute("create schema IF NOT EXISTS " + schema_name + " ;")
    cursor.execute("use schema " + schema_name + " ;")

    cursor.execute(script_creation)

    if (var_type == "csv"):
        creation_ff = "create or replace file format  CSV type = 'CSV'   SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = 'NONE' FIELD_DELIMITER = ';' RECORD_DELIMITER = '\n' error_on_column_count_mismatch=false NULL_IF = ('NULL') ;"
        cursor.execute(creation_ff)
    elif (var_type == "json"):
        creation_ff = " "
        cursor.execute(creation_ff)


def create_bucket_stor_int(cursor,name_bucket,schema_name):
    cursor.execute("CREATE OR REPLACE stage MARKET_PLACE_"+ schema_name  + " url = 'azure://srblobsnowflake.blob.core.windows.net/marketplace" + schema_name + "/' storage_integration = INTEGRATION_MARKETPLACE file_format = CSV ;")


def loading_data_from_bucket(cursor,schema_name,table_name,wh):

    cursor.execute("truncate " + table_name + ";")

    cursor.execute("Copy into "+ table_name + " from @MARKET_PLACE_" +  schema_name  + " purge=true;")

    cursor.execute("alter warehouse " + wh + " suspend;")


def get_url(schema_name):

    if (schema_name == "FINESS"):
        return get_url_finess()

    else :
        exit(0)





def get_url_finess():
    url = "https://www.data.gouv.fr/fr/datasets/finess-extraction-du-fichier-des-etablissements/"

    page = requests.get(url)    
    data = page.text
    soup = BeautifulSoup(data,features="html.parser")


    list_a = []
    for link in soup.find_all('a'):
        list_a.append(str(link))



    urls = []
    for e in list_a:
        if e.find('cs1100502') != -1:
            urls.append(e)

    print(urls)
    url = urls[0]

    url = url.split("')")[0]
    url = url.split("url=")[1]

    url = url.replace("%2F","/")
    url = url.replace("%3A",":")

    urls = []

    urls.append(url)

    return urls