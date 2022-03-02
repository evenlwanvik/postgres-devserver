from datetime import datetime, timedelta
import pandas as pd
import sqlalchemy
import logging
import lib

def migrate_delta(**kwargs):

    local_table = "agltransact_new"
    ext_table = "agltransact"
    
    local_db_conn = lib.local_db_connection(
        username="sa",
        password="Valhalla06978!",
        server="localhost,7000",
        db="testdata",
        driver="ODBC Driver 17 for SQL Server",
    )

    last_id = local_db_conn.execute(
        sqlalchemy.text(
            f"""
            SELECT MAX(agrtid) as last_id
            FROM {local_table}
            """
        )
    ).fetchall()[0][0]

    if last_id is None: 
        print(f"{local_table} is empty: terminating task.")
    else: 
        print(f"Last agrtid stored in local database table {local_table}: {last_id}.")
        
        ext_conn = lib.mssql_server_connection(server="AGR-DB17.sfso.no", db="AgrHam_PK01")

        query = f"""
            SELECT
                account,
                SUBSTRING(account, 1, 2) as acc_class,
                dim_1,
                dim_2,
                client,
                period,
                trans_date,
                agrtid,
                last_update
            FROM {ext_table} 
            WHERE agrtid > {last_id}
        """
        updated_rows = pd.read_sql_query(query, con=ext_conn)

        if updated_rows.empty:
            print(f"No new data from table {ext_table} in source database.")
        else:
            # Delete rows with duplicate agrtid before insert -
            # in case new last_update with existing agrtid
            agrtid_list = updated_rows["agrtid"].tolist()
            n_updates = len(agrtid_list)
            if len(agrtid_list) > 0:
                # TODO: Deleting single row is super slow, speed it up.
                for i, agrtid in enumerate(agrtid_list):
                    print(f"{i}/{n_updates}\tRemoving {agrtid}")
                    lib.delete_row(local_db_conn, local_table, agrtid)
            
            print("Adding updated rows to local db.")

            # TODO: Set to higher limits and chunksize when working on a more robust server
            # I doubt we ever will see more than 10000 rows updated in a table between runs though..
            if len(updated_rows) <= 10000:
                updated_rows.to_sql(
                    local_table, con=local_db_conn, if_exists="append", index=False)
            else:
                updated_rows.to_sql(
                    local_table, con=local_db_conn, if_exists="append", index=False, 
                    chunksize=1000)

            print(f"{n_updates} rows updated for table {local_table}.")
    
if __name__ == '__main__':
    migrate_delta()