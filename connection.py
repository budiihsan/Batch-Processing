import os
import json
import psycopg2

from sqlalchemy import create_engine

def config(connection_db):
    path = os.getcwd()
    with open(path+'/'+'config.json') as file:
        conf = json.load(file)[connection_db]
    return conf     

def psql_conn(conf, name_conn):
    try:
        conn = psycopg2.connect(
            host=conf['host'],
            database=conf['db'],
            user=conf['user'],
            password=conf['password'],
            port=conf['port']
    )   
        print(f'[INFO] Success connect postgreSQL{name_conn}')
        engine = create_engine(f"postgresql+psycopg2://{conf['user']}:{conf['password']}@{conf['host']}:{conf['port']}/{conf['db']}")
        return conn, engine
    
    except Exception as e:
        print(f"[INFO] Can't connect PostgreSQL")
        print(str(e))

#psql_conn(config())
