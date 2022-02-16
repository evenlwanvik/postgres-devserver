from re import T
from sqlalchemy import create_engine
import pandas as pd
import time
"""
AGRHAM db has one table per period with the budget of that period (month).
This code will sum up budget for a year for a specific client and account, 
which will be compared with the transactions from the general ledger.
"""

print("\n#################################### Connecting to databases ####################################\n")

src_uri = f"mssql+pyodbc://AGR-DB17.sfso.no/AgrHam_PK01?driver=ODBC+Driver+17+for+SQL+Server"
src_engine = create_engine(src_uri)


dest_uri = f"mssql+pyodbc://sa:Valhalla06978!@localhost:7000/testdata?driver=ODBC+Driver+17+for+SQL+Server"
dest_engine = create_engine(dest_uri, echo=False, fast_executemany=True)
dest_conn = dest_engine.connect()

tables = src_engine.table_names()
budget_tables = [t for t in tables if t.startswith('aagstd_2021')]
#budget_tables = ["aagstd_202110", "aagstd_202111", "aagstd_202112"]
print(f"Tables to migrate:")
print(*budget_tables, sep = ", ") 

print("\n#################################### Start migration ####################################\n")

sum = 0
for t in budget_tables:
    dest_conn.execute(f"DROP TABLE IF EXISTS {t}")
    print(f"\nMigrating table {t}")
    sql = f"SELECT * FROM dbo.{t};"
    chunksize = 100000
    for i, chunk in enumerate(pd.read_sql(sql, src_engine, chunksize=chunksize)):
        nrows = chunk.shape[0]
        print(f"({i+1}): Migrating row {i*chunksize} - {i*chunksize+chunksize}")
        df = pd.DataFrame(chunk.values, columns=chunk.columns)
        df.to_sql(t, dest_engine, if_exists='append', index=False)
        #time.sleep(20)
    time.sleep(1000)

src_engine.dispose()
dest_conn.close()
dest_engine.dispose()

