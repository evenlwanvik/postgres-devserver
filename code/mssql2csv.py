from sqlalchemy import create_engine
import pandas as pd
import os

src_uri = f"mssql+pyodbc://AGR-DB17.sfso.no/AgrHam_PK01?driver=ODBC+Driver+17+for+SQL+Server"
src_engine = create_engine(src_uri)

table = 'acrclient'

path_parent = os.path.dirname(os.getcwd())
os.chdir(path_parent)

sql = """
    SELECT *
    FROM [aplvitransact]
        where client in ('RR', 'AM', 'AL', 'DR')
        and [period] between 202001 and 202112   
"""

sql = f"""
    select * from {table}
"""

df = pd.read_sql(sql, src_engine)
fname = os.getcwd() + f'\data\{table}.csv'
df.to_csv(fname, sep='\t')

src_engine.dispose()