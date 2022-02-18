from sqlalchemy import create_engine, inspect
import pandas as pd

src_uri = f"mssql+pyodbc://AGR-DB17.sfso.no/AgrHam_PK01?driver=ODBC+Driver+17+for+SQL+Server"
src_uri = f"mssql+pyodbc://sa:Valhalla06978!@localhost:7000/testdata?driver=ODBC+Driver+17+for+SQL+Server"
src_engine = create_engine(src_uri)

tables = src_engine.table_names()

# Set account group
acc_grp = 50



###### Query aagstd_2021## for accounting data and insert into new table


###### Get the accounts used to produce accounting results in unit4 query

query = f"""
    select TOP 10000 * from 
    (
        SELECT 
            rr4dim2.rel_value AS "r4dim2", --- Drift/felles kostnader
            rr1dim1.rel_value AS "r1dim1", --- Kontogruppe
            t.dim1, --- Konto
            t.dim2, --- Koststed
            t.client, --- Klient
            SUM(t.cash_amount_ytd) AS cash_amount_ytd, --- Regnskap hittil i år (ytd=year today)
            SUM(t.pld_amount_y) AS pld_amount_y --- Årsbudsjett
        from aagstd_202108 t 
            INNER JOIN aglrelvalue rr4dim2 ON(t.client=rr4dim2.client AND rr4dim2.rel_attr_id='B3' AND rr4dim2.attribute_id='C1' AND t.dim2 BETWEEN rr4dim2.att_val_from and rr4dim2.att_val_to) INNER JOIN aglrelvalue rr1dim1 ON(t.client=rr1dim1.client AND rr1dim1.rel_attr_id='AH' AND rr1dim1.attribute_id='A0' AND t.dim1 BETWEEN rr1dim1.att_val_from and rr1dim1.att_val_to)

        where t.dim1 not in ('1049','1209','1239','1259','1269','1289','1294','1297','1298','1500','1501','1530','1540','1541','1542','1543','1545','1570','1571','1574','1575','1576','1577','1578','1579','1700','1750','1790','1931','1933','1939','1941','1949','1980','1985','1986','1987','1990','1991','1993','1997','1998','1999','2150','2190','2400','2401','2402','2408','2409','2600','2610','2615','2620','2630','2632','2640','2650','2690','2703','2780','2781','2782','2785','2786','2810','2820','2910','2915','2931','2932','2933','2934','2935','2936','2937','2938','2940','2941','2942','2960','2965','2980','2997','3030','3130','3235','3605','3850','3859','3900','3910','3950','3960','3970','3971','3972','5000','5003','5004','5005','5010','5012','5013','5080','5085','5090','5095','5096','5097','5100','5105','5108','5110','5112','5113','5180','5185','5190','5191','5192','5193','5200','5210','5220','5230','5240','5250','5252','5290','5300','5400','5410','5411','5412','5413','5414','5415','5416','5417','5420','5430','5710','5800','5801','5802','5810','5811','5812','5890','6000','6040','6041','6043','6044','6050','6070','6071','8480','8490') 
        AND rr1dim1.rel_value like '__' 
        AND t.client='RR'
        and rr1dim1.rel_value = '{acc_grp}'
        and rr4dim2.rel_value = 'DRIF'
        GROUP BY rr4dim2.rel_value,rr1dim1.rel_value,t.dim1,t.dim2,t.client
        HAVING SUM(t.pld_amount_y)<>0 OR SUM(t.cash_amount_ytd)<>0
    ) 
    x 
"""

df = pd.read_sql(query, src_engine)
kontoer = tuple(df['dim1'].unique())
koststed = tuple(df['dim2'].unique())
print("kontoer:", kontoer)
print("koststed:", koststed)

###### Get the relevant transactions from GL on these accounts

perioder = tuple([f"20210{i}" for i in range(0,9)])

query = """
    SELECT sum(amount) as sum
    FROM dbo.agltransact
    where client = 'RR'
    and account in %s 
    and dim_1 in %s
    and trans_date between '2020-12-31 00:00:00.000' and '2021-09-01 00:00:00.000'
""" % (kontoer, koststed)
#% (",".join(["?"]*len(kontoer)), ",".join(["?"]*len(koststed)), ",".join(["?"]*len(perioder)))

print(pd.read_sql(query, src_engine))


src_engine.dispose()