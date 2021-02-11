import pandas as pd
import numpy as np
import pandahouse as ph
from datetime import date
#import holidays
#from fbprophet import Prophet

import datetime
import random
import prefect
from prefect.engine.executors import DaskExecutor
from prefect.utilities.notifications import slack_notifier

from datetime import datetime, timedelta
from prefect import Flow
from prefect.schedules import IntervalSchedule

from prefect import Client

# Input parameter for CSV filename on Amazon S3
connection = dict(database='default',
                  host='http://clickhouse.beslan.pro:8080',
                  user='student',
                  password='dpo_python_2020')


# Our normal Prefect flow
@prefect.task(max_retries=3, retry_delay=timedelta(minutes=1))
def download(connection):
    query = """
    SELECT CAST(BuyDate AS Date) AS SalesDate, UserID, DeviceID, Rub FROM checks  AS l
    LEFT JOIN (SELECT * FROM devices) AS r ON l.UserID=r.UserID
    LIMIT 100000
    """
    df = ph.read_clickhouse(query, connection=connection)
    return df


@prefect.task(max_retries=5, retry_delay=timedelta(seconds=2))
def agregate(df):
    agg = df.groupby(['SalesDate', 'UserID'], as_index=False).sum()
    return agg


# schedule to run every 12 hours
schedule = IntervalSchedule(
    start_date=datetime.utcnow() + timedelta(seconds=1),
    # interval=timedelta(hours=12))
    interval=timedelta(minutes=5),
    end_date=datetime.utcnow() + timedelta(minutes=10))

with prefect.Flow(
        name="SQL",
        schedule=schedule,
        # state_handlers=[handler],
) as flow:
    dataframes = download(connection)
    fin = agregate(dataframes)

#
client = Client()
client.create_project(project_name='SQL')
flow.register(project_name='SQL')
#
flow.run()