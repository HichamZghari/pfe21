from utils import *



def load(name):

    path = "ini_files/" + name + ".ini"


    ## config for ini file
    config = configparser.ConfigParser()
    config.sections()
    config.read(path)

    ## get info from ini file
    schema_name, urls, table_name, script_creation, separateur, chunk, name_columns, file_type, row_to_skip, enco, database = get_info_from_ini_file(config)

    
    ## if you need to find the urls
    if urls[0] == "":
        urls = get_url(schema_name)


    ## config for snowflake
    config_SF = configparser.ConfigParser()
    config_SF.sections()
    config_SF.read("snowflake.ini")



    ## get informations from snowflake file
    account,user,password,warehouse = get_informations_SF(config_SF)



    ### later get the urls if needed


    nb = 0


    name_bucket = "atos_market_place"


    for url in urls:
    
        data = get_data(url)

        nb = handle_decompose_save(data,url,name_bucket,separateur,table_name,nb,chunk,name_columns,file_type,row_to_skip,enco,config,schema_name)



    ## connexion snowflake 
    sf_cursor, connexion = create_connexion_snowflake(account,user,password,warehouse)


    var_type = "csv"

    ## creation objet snowflake
    create_objects_sf(sf_cursor,schema_name,script_creation,var_type,table_name,database)


    ## creation bucket and storage integration
    create_bucket_stor_int(sf_cursor,name_bucket,schema_name)

    ## load data 
    loading_data_from_bucket(sf_cursor,schema_name,table_name,warehouse)