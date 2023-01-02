import os
import connection
import sqlparse
import pandas as pd


if __name__ == '__main__':
    print('[INFO] Service ETL is Starting ...')
    
    # connection data source
    conf = connection.config('marketplace_prod')
    conn, engine = connection.psql_conn(conf, 'DataSource')
    cursor = conn.cursor()

    # connection dwh
    conf_dwh_migration = connection.config('dwh_migration')
    conn_dwh_migration, engine_dwh_migration = connection.psql_conn(conf_dwh_migration, 'DataWarehouse')
    cursor_dwh_migration = conn_dwh_migration.cursor()

    # get query string
    path_query = os.getcwd()+'/query/'
    query = sqlparse.format(
        open(path_query+'query.sql', 'r').read(), strip_comments=True
    ).strip()

    # get schema dwh design
    path_dwh_migration_design = os.getcwd()+'/query/'
    dwh_migration_design = sqlparse.format(
        open(path_dwh_migration_design+'dwh_migration_design.sql', 'r').read(), strip_comments=True
    ).strip()

    try:
        # get data
        print('[INFO] Service ETL is Running ...')
        df = pd.read_sql(query, engine)
        
        # create schema dwh
        cursor_dwh_migration.execute(dwh_migration_design)
        conn_dwh_migration.commit()

        # ingest data to dwh
        df.to_sql('dim_orders', engine_dwh_migration, if_exists='append', index=False)
        print('[INFO] Service ETL is Success ...')
    except Exception as e:
        print('[INFO] Service ETL is Failed ...')