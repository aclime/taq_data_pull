import math
import numpy as np
import pandas as pd

from box_client import make_client
client=make_client()

import wrds
db = wrds.Connection(autoconnect=True)

def get_crsp_df(year,month):

    crsp_df=db.raw_sql(f"""select *
                        from crsp_a_stock.dsf
                        where    extract(year from date)={year}
                                    and extract(month from date)={month}
                    """
            ,date_cols=['date'])

    mse_df=db.raw_sql(f"""select *
                from crsp_a_stock.mseall 
                where    extract(year from date)={year}
                        and extract(month from date)={month}
                    """
            ,date_cols=['date'])
    mse_df=mse_df.groupby(['permno','permco']).tail(1)
    mse_df.drop(['cusip','hexcd','issuno','hsiccd','date','shrout'],inplace=True,axis=1)

    sp500_df=db.raw_sql(f"""select *
                from crsp.dsp500list
                    """
            ,date_cols=['start','ending'])
    sp500_df['sp_permno']=sp500_df['permno'].copy()

    compno_df=db.raw_sql(f"""select *
                from crsp.dse
                where    extract(year from date)={year}
                            and extract(month from date)={month}
                    """
            ,date_cols=['date'])
    compno_df.dropna(subset=['compno'],inplace=True)
    compno_df=compno_df[['permno','compno']].drop_duplicates(subset=['permno'])

    crsp_df_2=crsp_df.merge(mse_df,how='left',on=['permno','permco'])
    crsp_df_2['permno_dupe']=crsp_df_2['permno'].copy()
    sp500_df_2=sp500_df.copy()
    sp500_df_2['interval']=sp500_df_2.apply(lambda x:(x.start,x.ending),axis=1)
    sp500_df_3=sp500_df_2.groupby(['permno'])['interval'].agg(list).reset_index()
    crsp_df_3=crsp_df_2.merge(sp500_df_3,how='left',on='permno')

    columns_needed=['cusip', 'permno', 'permco', 'issuno', 'hexcd', 'hsiccd', 'date',
       'bidlo', 'askhi', 'prc', 'vol', 'ret', 'bid', 'ask', 'shrout', 'cfacpr',
       'cfacshr', 'openprc', 'numtrd', 'retx', 'nameendt', 'shrcd',
       'exchcd', 'siccd', 'ncusip', 'ticker', 'comnam', 'shrcls', 'tsymbol',
       'naics', 'primexch', 'trdstat', 'secstat', 'compno', 'sp500',
       'year', 'month']

    def in_sp500(row):
        intervals=row['interval']
        day=row['date']
        if  type(intervals)!=list:
            return 0
        else:
            in_index=0
            for interval in intervals:
                if day>=interval[0] and day<=interval[1]:
                    in_index=1
                    break
            return in_index

    crsp_df_3['sp500']=crsp_df_3.apply(in_sp500,axis=1)
    crsp_df_3.drop(['permno_dupe','interval'],axis=1,inplace=True)
    crsp_df_4=crsp_df_3.merge(compno_df,how='left',on='permno')
    crsp_df_4=crsp_df_4[columns_needed].sort_values(by=['permno','permco'])
    crsp_df_4['date']=crsp_df_4['date'].dt.strftime('%Y%m%d')
    crsp_df_4 = crsp_df_4.astype({'shrout':int})
    return crsp_df_4


