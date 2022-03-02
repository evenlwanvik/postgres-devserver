import sqlalchemy

def mssql_server_connection(
    server, db, driver="ODBC Driver 17 for SQL Server"
    ) -> sqlalchemy.engine.base.Engine:
    """
    Returns a connection to a mssql server (microsoft login)
    """
    conn_url = f"mssql://@{server}/{db}?driver={driver}"
    engine = sqlalchemy.create_engine(conn_url, fast_executemany=True)

    return engine.connect()
     
def local_db_engine(
    username, password, server, db, driver="ODBC Driver 17 for SQL Server"
    ) -> sqlalchemy.engine.base.Engine:
    """ 
    Returns an sqlalchemy engine to local mssql instance (username/password login)
    """
    conn_url = f"mssql://{username}:{password}@{server}/{db}?driver={driver}"
    
    return sqlalchemy.create_engine(conn_url, fast_executemany=True)

def local_db_connection(
    username, password, server, db, driver="ODBC Driver 17 for SQL Server"
    ) -> sqlalchemy.engine.base.Connection:
    """ 
    Returns a connection to local mssql instance (username/password login)
    """
    conn_url = f"mssql://{username}:{password}@{server}/{db}?driver={driver}"
    engine = sqlalchemy.create_engine(conn_url, fast_executemany=True)
    
    return engine.connect()


def delete_row(db_conn: sqlalchemy.engine.base.Connection, table: str, agrtid: int) -> None:
    """
    Delete row with given agrtid
    """
    db_conn.execute(
        sqlalchemy.text(
            f"""
            DELETE FROM {table}
            WHERE agrtid = {agrtid}
            """
        )
    )

LOCAL_DB_CONNECTION = local_db_connection(
    username="sa",
    password="Valhalla06978!",
    server="localhost,7000",
    db="testdata",
    driver="ODBC Driver 17 for SQL Server",
)

LOCAL_DB_ENGINE = local_db_engine(
    username="sa",
    password="Valhalla06978!",
    server="localhost,7000",
    db="testdata",
    driver="ODBC Driver 17 for SQL Server",
)