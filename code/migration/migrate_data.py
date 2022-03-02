from sqlalchemy import create_engine
import pandas as pd
import time
import math

src_uri = f"mssql+pyodbc://AGR-DB17.sfso.no/AgrHam_PK01?driver=ODBC+Driver+17+for+SQL+Server"
src_engine = create_engine(src_uri)

dest_uri = f"mssql+pyodbc://sa:Valhalla06978!@localhost:7000/testdata?driver=ODBC+Driver+17+for+SQL+Server"
dest_engine = create_engine(dest_uri, echo=False, fast_executemany=True)

# Set a list of table to be extracted from source
table_name = "aplvitransact"

period_from_to = [201901, 202212]

def remove_table():
    dest_conn = dest_engine.connect()
    # Drop table if exists
    dest_conn.execute(f"DROP TABLE IF EXISTS {table_name}")
    dest_conn.close()

def number_of_rows():
    sql = f"""
        SELECT COUNT([user_id]) AS nrows 
        FROM [AgrHam_PK01].[dbo].{table_name}
        where period between {period_from_to[0]} and {period_from_to[1]}
    """
    df = pd.read_sql(sql, src_engine)
    return df["nrows"][0]


def migrate_data():

    print("\n#################################### Start queries ####################################\n")

    print("\nEngines created")

    # Hold count of number of rows to iterate
    nrows = number_of_rows()
    print(f"Number of rows to migrate: {nrows}")
    sql = f"""
        SELECT * 
        FROM [AgrHam_PK01].[dbo].{table_name}
        where period between {period_from_to[0]} and {period_from_to[1]}
    """
    chunksize = 50000
    for i, chunk in enumerate(pd.read_sql(sql, src_engine, chunksize=chunksize)):
        print(f"({i+1}/{math.ceil(nrows/chunksize)}): Migrating row {i*chunksize} - {i*chunksize+chunksize}")
        df = pd.DataFrame(chunk.values, columns=chunk.columns)
        df.to_sql(table_name, dest_engine, if_exists='append', index=False)
        time.sleep(30)


if __name__ == '__main__':
    remove_table()
    #print(number_of_rows())
    migrate_data()

# Close connections and dispose engines
src_engine.dispose()
dest_engine.dispose()

print("\nEngines disposed")