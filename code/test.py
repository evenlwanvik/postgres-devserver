from datetime import datetime
import pandas as pd
import sqlalchemy
import hashlib
import math

# TODO: Replace hard-code variable
start_date = datetime(2022, 3, 15)


def mssql_db_conn(
    username=None, password=None, host=None, db=None, port=None, driver="ODBC Driver 17 for SQL Server", trusted_connection=False 
    ) -> sqlalchemy.engine.base.Connection:
    """ 
    Returns a connection to local mssql instance (username/password login)
    """
    if trusted_connection:
        conn_url = f"mssql+pyodbc://@{host}/{db}?driver={driver}&truster_connection=yes"
    else:  
        conn_url = f"mssql+pyodbc://{username}:{password}@{host}:{port}/{db}?driver={driver}"
  
    print(f"connecting to {conn_url}")

    engine = sqlalchemy.create_engine(conn_url, fast_executemany=True)

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

    initial_period = 201701

    table = "agltransact"

    local_db_conn = mssql_db_conn(
                        username="sa",
                        password="Valhalla06978!",
                        host="172.17.0.1",
                        port="7000",
                        db="master",
                        driver="ODBC+Driver+17+for+SQL+Server"
    )

    #external_db_conn = mssql_db_conn(host="AGR-DB17.sfso.no", db="AgrHam_PK01", trusted_connection=True)

    previous_period = pd.read_sql_query(
        f"""
            SELECT
                MAX(period)
            FROM {table} 
        """, 
        con=local_db_conn
    ).fetchall()[0][0]

    if previous_period is None:
        print(f"first migration, no data in local database, initializing period")
        previous_period = initial_period

    period = next_month(previous_period)

    print(period)

migrate_delta()