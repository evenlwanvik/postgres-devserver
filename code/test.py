from datetime import datetime
import pandas as pd
import sqlalchemy
import hashlib
import math
import logging
import time

# TODO: Replace hard-code variable
start_date = datetime(2022, 3, 15)


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

    return engine.connect()


def next_month(current_period):
    """
    Input period in yyyymm format and returns next period in yyyymm format
    """
    year, month = int(current_period[:4]), int(current_period[4:])
    month = year*12+month+1 # Next month
    year, month = divmod(month-1, 12)

    return "%d%02d" % (year, month+1) # Final +1 for 12%12 == 0


def migrate_delta():

    initial_period = "201701"
    period = initial_period

    table = "agltransact"

    local_db_conn = pgsql_server_connection(
                        username="admin",
                        password="admin",
                        host="172.17.0.1",
                        port="7000",
                        db="DataPlatform01"
    )

    #sql = sqlalchemy.text(f'DROP TABLE IF EXISTS {table};')
    #local_db_conn.execute(sql)

    try:
        previous_period = pd.read_sql_query(
        f"""
            SELECT
                MAX(period) as last_period
            FROM {table} 
        """, 
        con=local_db_conn
        )["last_period"][0]

        period = next_month(str(previous_period))

    except sqlalchemy.exc.SQLAlchemyError:
        print("table does not exist. setting initial period.")
        previous_period = initial_period
        pass

    print(period)

migrate_delta()