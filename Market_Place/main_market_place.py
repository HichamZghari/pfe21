from utils_market_place import *
import os
import shutil
import configparser
from sys import argv, exit


if len(argv) != 2 :
    print("error need an ini file as parameter")
    exit(1)

## get path ini file
path = argv[1]


## config for ini file
config = configparser.ConfigParser()
config.sections()
config.read(path)

## get info from ini file
schema_name, urls, table_name, script_creation, separateur, chunk, name_columns, file_type, row_to_skip, enco = get_info_from_ini_file(config)


directory = 'C://Users//a809310//Desktop//temp//'

schema_name = config["Data"]["schema"]

## config for snowflake
config_SF = configparser.ConfigParser()
config_SF.sections()
config_SF.read("snowflake.ini")



## get informations from snowflake file
account,user,password,warehouse = get_informations_SF(config_SF)



directory = 'C://Users//a854199//Desktop//' + table_name +'//'

## if directory exist delet it
if os.path.exists(directory):
    shutil.rmtree(directory)


## create directory
os.mkdir(directory)


nb = 0
## pass throught urls

for url in urls:
    
    data = get_data(url)

    nb, var_type = handle_decompose_save(data,url,directory,separateur,table_name,nb,chunk,name_columns,file_type,row_to_skip,enco,config)





## connexion snowflake 
sf_cursor, connexion = create_connexion_snowflake(account,user,password,warehouse)

## creation objet snowflake
create_objects_sf(sf_cursor,schema_name,script_creation,var_type,table_name)

## load and insert into snowflake
load_into_table(sf_cursor, table_name, schema_name,var_type, directory,warehouse)


## close connexion
connexion.close()

## delete directory
shutil.rmtree(directory)

