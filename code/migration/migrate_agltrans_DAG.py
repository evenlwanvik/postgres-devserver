from datetime import datetime
import pandas as pd
import sqlalchemy
import math
import time
import os
import psycopg2
import numpy as np

# TODO: Replace hard-code variable
start_date = datetime(2022, 3, 21)

def mssql_db_conn(
        username=None, 
        password=None, 
        host=None, 
        db=None, 
        port=None, 
        driver="ODBC Driver 17 for SQL Server", 
        trusted_connection=False 
    ) -> sqlalchemy.engine.base.Connection:
    """ 
    Returns a connection to local mssql instance (username/password or mssql)
    """

    if trusted_connection:
        conn_url = f"mssql+pyodbc://@{host}/{db}?driver={driver}&truster_connection=yes"
    else:  
        conn_url = f"mssql+pyodbc://{username}:{password}@{host}:{port}/{db}?driver={driver}"

    engine = sqlalchemy.create_engine(conn_url, fast_executemany=True)

    return engine.connect()


def pgsql_server_connection(
    username, password, host, port, db
    ) -> sqlalchemy.engine.base.Connection:
    """
    Returns a connection to a postgresql server (username/password password login)
    """

    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{db}"
    engine = sqlalchemy.create_engine(conn_url, executemany_mode="batch")

    return engine.raw_connection()


def next_month(current_period):
    """
    Input period in yyyymm format and returns next period in yyyymm format
    """
    year, month = int(current_period[:4]), int(current_period[4:])
    month = year*12+month+1 # Next month
    year, month = divmod(month-1, 12)

    return "%d%02d" % (year, month+1) # Final +1 for 12%12 == 0


def copy_from_file(conn, df, table):
    """
    Save Pandas dataframe on disk as a csv file, load the csv file  
    and use copy_from() to copy it to the table
    """
    print(pd.__version__)
    # Save the dataframe to disk
    tmp_df = "./tmp_dataframe.csv" 
    csv_file_delimeter=":~:"
    np.savetxt(tmp_df, df, delimiter=csv_file_delimeter, header=csv_file_delimeter.join(df.columns.values), fmt='%s', comments='', encoding=None)
    #df.to_csv(tmp_df, sep =";~;", encoding='utf-8', index=False, header=False)
    f = open(tmp_df, 'r')
    with conn.connection.cursor() as cursor:
        try:
            cursor.copy_from(f, table, sep=";~;")
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            os.remove(tmp_df)
            print("Error: %s" % error)
            conn.rollback()
            cursor.close()
            return 1
    print("copy_from_file done")
    os.remove(tmp_df)


def execute_batch(conn, df, table, page_size=100):
    """
    Using psycopg2.extras.execute_batch() to insert the dataframe
    """
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]

    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    vals_frame = len(cols) * """%%s,"""
    vals_frame = vals_frame[:-1]
    print('vals_frame: ',vals_frame)
    
    query  = "INSERT INTO %s(%s) VALUES("+vals_frame+")" % (table  , cols)
    cursor = conn.cursor()
    try:
        psycopg2.extras.execute_batch(cursor, query, tuples, page_size)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("psycopg2 execute_batch done")
    cursor.close()


def migrate_delta():

    initial_period = "201701"
    period = initial_period

    table = "agltransact"
    local_table = "test"

    local_db_conn = pgsql_server_connection(
                        username="admin",
                        password="admin",
                        host="172.17.0.1",
                        port="7000",
                        db="DataPlatform01"
    )

    try:
        previous_period = pd.read_sql_query(
        f"""
            SELECT
                MAX(period) as last_period
            FROM {local_table} 
        """, 
        con=local_db_conn
        )["last_period"][0]
        
        period = next_month(str(previous_period))

    except sqlalchemy.exc.SQLAlchemyError:
        print("table does not exist. setting initial period.")
        previous_period = initial_period
        pass

    external_db_conn = mssql_db_conn(host="AGR-DB17.sfso.no", db="AgrHam_PK01", trusted_connection=True)

    n_updated_rows = pd.read_sql_query(
            f"""
                SELECT 
                    count(account) as nRows
                FROM {table} 
                where period = {period}
            """, 
            con=external_db_conn
        )["nRows"][0]

    print(f"{n_updated_rows} from period {period} for {table} to be migrated to local db", flush=True)

    query = f"""
        SELECT 
            *
        FROM {table}
        WHERE period = {period}
    """

    start_time = time.time()

    #chunksize = 50000
    #wait_time = 20
    #print(f"waiting {wait_time} seconds between chunks of {chunksize} rows", flush=True)
    #for i, chunk in enumerate(pd.read_sql_query(query, con=external_db_conn, chunksize=chunksize)):
    #    print(f"({i+1}/{math.ceil(n_updated_rows/chunksize)}): Migrating row {i*chunksize} - {i*chunksize+chunksize}")
    #    df = pd.DataFrame(chunk.values, columns=chunk.columns)
    #    df.to_sql(table, con=local_db_conn, if_exists='append', index=False)#, method='multi')
    #    time.sleep(wait_time)
    #migration_time = time.time() - start_time - wait_time*(i+1)
    
    df = pd.read_sql_query(query, con=external_db_conn)
    #copy_from_file(local_db_conn, df, table)
    execute_batch(local_db_conn, df, local_table, page_size=50000)
    migration_time = time.time() - start_time

    print(f"Rows successfully migrated in {migration_time} seconds", flush=True)


migrate_delta()