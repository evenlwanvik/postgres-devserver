from sqlalchemy import create_engine
import pandas as pd

"""
AGRHAM db has one table per period with the budget of that period (month).
This code will sum up budget for a year for a specific client and account, 
which will be compared with the transactions from the general ledger.
"""

print("\n#################################### Connecting to databases ####################################\n")

src_uri = f"mssql+pyodbc://sa:Valhalla06978!@localhost:7000/testdata?driver=ODBC+Driver+17+for+SQL+Server"
src_engine = create_engine(src_uri, echo=False, fast_executemany=True)
src_conn = src_engine.connect()

tables = src_engine.table_names()
budget_tables = [t for t in tables if t.startswith('aagstd_202')]

print(f"Tables to delete:")
print(*budget_tables, sep = ", ") 

print("\n#################################### Deleting tables ####################################\n")

for t in budget_tables:
    src_conn.execute(f"DROP TABLE IF EXISTS {t}")

src_conn.close()
src_engine.dispose()

