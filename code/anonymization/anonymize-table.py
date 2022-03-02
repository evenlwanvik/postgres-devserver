from sqlalchemy import create_engine
import pandas as pd
import time

uri = f"mssql+pyodbc://AGR-DB17.sfso.no/AgrHam_PK01?driver=ODBC+Driver+17+for+SQL+Server"
engine = create_engine(uri)

