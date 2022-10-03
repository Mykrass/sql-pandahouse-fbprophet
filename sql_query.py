import pandas as pd
import numpy as np
import pandahouse as ph
from datetime import date
import holidays
from fbprophet import Prophet


# Input parameter for CSV filename on Amazon S3
connection = dict(database='default',
                  host='http://clickhouse.beslan.pro:8080',
                  user='student',
                  password='dpo_python_2020')


#
def download(connection):
    query = """
    SELECT CAST(BuyDate AS Date) AS SalesDate, UserID, DeviceID, Rub FROM checks  AS l
    LEFT JOIN (SELECT * FROM devices) AS r ON l.UserID=r.UserID
    LIMIT 10000
    """
    df = ph.read_clickhouse(query, connection=connection)
    return df


#
def agregate(df):
    agg = df.groupby(['SalesDate', 'UserID'], as_index=False).sum()
    return agg

#
df = download(connection)
df_agg = agregate(df)
df_agg
