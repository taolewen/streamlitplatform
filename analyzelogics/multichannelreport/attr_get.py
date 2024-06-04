from urllib.parse import quote_plus

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine

host = st.secrets["mysql"]['host'],
user = st.secrets["mysql"]['user'],
password = st.secrets["mysql"]['password'],
db = st.secrets["mysql"]['database']

def get_week():
    connstr = f"mysql+pymysql://{user[0]}:%s@{host[0]}:3306/{db}?charset=utf8" % quote_plus(f'{password[0]}')
    engine = create_engine(connstr)
    sql=f'''select week,start_date,end_date,qijian from multichannel_oc_dateweek where end_date<=curdate() order by week desc '''
    df=pd.read_sql(sql,con=engine)
    df['weekinfo']=df.apply(lambda x:str(x.week)+' : '+str(x.start_date)+'~'+str(x.end_date),axis=1)
    # df=df[['weekinfo']]

    list=df.to_dict(orient='list')
    print(list)
    # def getw(d):
    #     wi=d['weekinfo']
    #     return wi
    # list=map(getw,list)
    return list
def get_qijian(week):
    connstr = f"mysql+pymysql://{user[0]}:%s@{host[0]}:3306/{db}?charset=utf8" % quote_plus(f'{password[0]}')
    engine = create_engine(connstr)
    sql=f'''select week,start_date,end_date,qijian from multichannel_oc_dateweek where week = '{str(week)}' '''
    df=pd.read_sql(sql,con=engine)
    qijian=df['qijian'].values[0]
    return qijian
def get_periodrange(week):
    connstr = f"mysql+pymysql://{user[0]}:%s@{host[0]}:3306/{db}?charset=utf8" % quote_plus(f'{password[0]}')
    engine = create_engine(connstr)
    sql=f'''select week,start_date,end_date,qijian from multichannel_oc_dateweek where week = '{str(week)}' '''
    df=pd.read_sql(sql,con=engine)
    start_date=df['start_date'].values[0]
    end_date=df['end_date'].values[0]
    return str(start_date)+'~'+str(end_date)
def get_option(channel):
    opdict={
        'Wayfair':{ #CG发货订单
            'reporttype':['已发货订单','广告','付款','付款_新','仓租&配送费invoice','CG发货订单','头程invoice'],
            'country':[],
            'area': ['US','EU','CA'],
            'store': ['WF-EU-2','WF-EU-5','WF-US-1','WF-US-2','WF-US-5','WF-CA-1','WF-Wellynap']


        },
        'Ebay': {
            'reporttype': ['transaction','orders'],
            'country': ['US','DE','UK'],
            'area': ['US','EU'],
            'store': ['EBAY-DE-2','EBAY-UK-2','EBAY-US-2']

        },
        'CD': {
            'reporttype': ['orderextract','paymentdetail_回款','paymentdetail_结算'],
            'country': ['FR'],
            'area': ['EU'],
            'store': ['CD-1','CD-3','CD-6','CD-HYL','CD-HEMQ']

        },
        'manomano': {
            'reporttype': ['广告','订单','退款','回款','平台仓租','平台物流费'],
            'country': ['FR','DE','ES','IT'],
            'area': ['EU'],
            'store': ['MM-2','MM-3','MM-3-DE','MM-3-MF','MM-HEMQ','MM-HEMQ-MF','MM-3-ES','MM-3-IT']

        },
        'walmart': {
            'reporttype': ['订单','广告','订单—jd','付款payment','退货','结算(运费)'],
            'country': ['US'],
            'area': ['US'],
            'store': ['WM-JD','WM-2','WM-5','WM-6','WM-7','WM-Wellynap']

        },
        '独立站': {
            'reporttype': ['订单','广告1_google','广告2_bing'],
            'country': ['US','DE','UK','CA'],
            'area': ['US','EU'],
            'store': ['独立站-DE','独立站-US','独立站-UK','独立站-CA']

        },
        'shein': {
            'reporttype': ['订单', '回款'],
            'country': ['US', 'DE'],
            'area': ['US', 'EU'],
            'store': ['Shein-US','Shein-EU','SheinKSBDHOME-US']
        },
        'overstock': {
            'reporttype': ['订单', '回款'],
            'country': ['US'],
            'area': ['US'],
            'store': ['overstock-US']
        },
        'temu': {
            'reporttype': ['订单','账单明细','结算'],
            'country': ['US'],
            'area': ['US'],
            'store': ['temu-US']
        }
    }
    return opdict[channel]
