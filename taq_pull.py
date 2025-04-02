import math
import numpy as np
import pandas as pd

from wrds_creds import read_pgpass
wrds_username,wrds_password=read_pgpass()
import wrds
db = wrds.Connection(wrds_host="wrds-pgdata.wharton.upenn.edu",
                     wrds_username=wrds_username,
                     wrds_password=wrds_password)

def get_nbbo_tick_by_day_old(ticker,day):

    nbbo_df=db.raw_sql(f"""
    SELECT
        SUBSTR(time_m::VARCHAR,1,5) AS time_m,
        best_bid,
        ROUND(hr_avg_prc,2) AS hr_avg_pr,
        best_bid - ROUND(hr_avg_prc,2) AS diff
    FROM
        (
            SELECT
                *,
                RANK() OVER (PARTITION BY sym_root, date, EXTRACT (HOUR FROM time_m), EXTRACT (MINUTE FROM time_m) ORDER BY time_m, time_m_nano) AS minute_rank,
                AVG(best_bid) OVER (PARTITION BY sym_root, date, EXTRACT (HOUR FROM time_m)) AS hr_avg_prc
            FROM
                taqm_2022.complete_nbbo_2022
            WHERE
                sym_root = '{ticker}' AND
                date = '{day}' 
        ) a
    WHERE
        minute_rank = 1
    ORDER BY
        1
     """)
    return nbbo_df
         #   ,date_cols=['date'])


def get_nbbo_tick_by_day(ticker,day):

    nbbo_df=db.raw_sql(f"""
    SELECT
        *
    FROM
        taqm_2022.complete_nbbo_2022
    WHERE
        sym_root = '{ticker}' AND
        date = '{day}' AND
        EXTRACT (HOUR FROM time_m) = 9
     """)
    return nbbo_df