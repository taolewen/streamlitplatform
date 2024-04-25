
#cd
import datetime
import os
import traceback
import uuid
from datetime import timedelta
from urllib.parse import quote_plus

import pymysql
from dateutil import parser
import pandas as pd

# 显示所有列
from sqlalchemy import create_engine


import streamlit as st
host = st.secrets["mysql"]['host'],
user = st.secrets["mysql"]['user'],
password = st.secrets["mysql"]['password'],
db = st.secrets["mysql"]['database']
connstr = f"mysql+pymysql://{user[0]}:%s@{host[0]}:3306/{db}?charset=utf8" % quote_plus(f'{password[0]}')
engine = create_engine(connstr)
pd.set_option('display.max_columns', None)
reporttype='dataextract_temu_settled'

def getuid():
    uid = str(uuid.uuid4())
    suid = ''.join(uid.split('-'))
    return suid
def updatebatch(attrjson,batchid,path):
    conn = pymysql.connect(host=st.secrets["mysql"]['host'],
                                user=st.secrets["mysql"]['user'],
                                password=st.secrets["mysql"]['password'],
                                db=st.secrets["mysql"]['database'])
    cursor = conn.cursor()
    sql = f"""insert newchannel_batchinfo (batchid,reporttype,path,area,country,week,store,qijian) values 
    ('{batchid}','{reporttype}','{path}','{attrjson['area'].upper()}','{(attrjson['country']).upper()}',
    '{attrjson['week']}','{attrjson['store'].upper()}','{attrjson['qijian']}')"""
    cursor.execute(sql)
    conn.commit()
    cursor.close()
    conn.close()
def selectbatch(attrjson):
    sql = f"""select * from newchannel_batchinfo where reporttype='{reporttype}' 
    {f'''and area='{attrjson['area']}' ''' if attrjson['area'] else ''}
    {f'''and country='{attrjson['country']}' ''' if attrjson['country'] else ''}
    {f'''and store='{attrjson['store']}' ''' if attrjson['store'] else ''}
    {f'''and week='{attrjson['week']}' ''' if attrjson['week'] else ''}
    {f'''and qijian='{attrjson['qijian']}' ''' if attrjson['qijian'] else ''}
    order by createdate desc"""
    df=pd.read_sql(sql,con=connstr)
    dl=df.to_dict('records')
    strlist=[]
    for d in dl:
        try:
            filename=d['path'].split('/')[-1]
        except:
            filename='notfound'
        str=f"{d['createdate']}_{filename}_{d['area']}_{d['country']}_{d['store']}_{d['week']}_{d['batchid']}"
        # print(str)
        strlist.append(str)
    def getfilename(x):
        try:
            return x.split('/')[-1]
        except:
            return 'none'
    df['filename']=df.apply(lambda x:getfilename(x.path),axis=1)
    df=df.drop('path', axis=1)
    df['delete']=False
    df=df[['delete','area','country','qijian','week','store','filename','createdate','reporttype','batchid']]
    return df
def deletebatch(batchid):
    try:
        conn = pymysql.connect(host=st.secrets["mysql"]['host'],
                               user=st.secrets["mysql"]['user'],
                               password=st.secrets["mysql"]['password'],
                               db=st.secrets["mysql"]['database'])
        cursor = conn.cursor()
        sql0 = f"""delete from  newchannel_temu_salesincome where batchid = '{batchid}' """
        cursor.execute(sql0)
        sql1 = f"""delete from  newchannel_temu_salesrefund where batchid = '{batchid}' """
        cursor.execute(sql1)
        sql2 = f"""delete from  newchannel_temu_transfeeincome where batchid = '{batchid}' """
        cursor.execute(sql2)
        sql3 = f"""delete from  newchannel_temu_transfeerefund where batchid = '{batchid}' """
        cursor.execute(sql3)
        sql = f"""delete from  newchannel_batchinfo where batchid = '{batchid}' """
        cursor.execute(sql)
        conn.commit()
        cursor.close()
        conn.close()
        return 1,''
    except:
        return 2,traceback.format_exc()
def dealsinglefile(path,attrjson):
    try:
        batchid=getuid()
        # name=['gsp_orderid','sellersku','pro_price','username','postcode','country','province','city','user_address1','user_address2','phonenum']
################################
        name=['订单编号','子订单号','sku明细','交易收入','币种','账务时间']
        df_salesincome = pd.read_excel(path,sheet_name='交易收入', usecols='A:F', names=name)
        df_salesincome['sku明细']=''
        df_salesincome['area'] = attrjson['area']
        df_salesincome['country'] = attrjson['country']
        df_salesincome['store'] = attrjson['store']
        df_salesincome['week'] = attrjson['week']
        df_salesincome['qijian']=attrjson['qijian']
        df_salesincome['batchid']=batchid

        df_salesincome=df_salesincome[['area','country','store','week','qijian',
                                       '订单编号', '子订单号', 'sku明细', '交易收入', '币种', '账务时间',
                                       "batchid"
        ]]
        df_salesincome.to_sql('newchannel_temu_salesincome', con=engine, if_exists='append', index=False, index_label=False)
###############################
        name=['售后单号','订单编号','子订单号','sku明细','售后退款金额','币种','账务时间']
        df_salesrefund = pd.read_excel(path,sheet_name='售后退款', usecols='A:G', names=name)
        df_salesrefund['area'] = attrjson['area']
        df_salesrefund['country'] = attrjson['country']
        df_salesrefund['store'] = attrjson['store']
        df_salesrefund['week'] = attrjson['week']
        df_salesrefund['qijian']=attrjson['qijian']
        df_salesrefund['batchid']=batchid

        df_salesrefund=df_salesrefund[['area','country','store','week','qijian',
                                       '售后单号','订单编号','子订单号','sku明细','售后退款金额','币种','账务时间',
                                       "batchid"
        ]]
        df_salesrefund.to_sql('newchannel_temu_salesrefund', con=engine, if_exists='append', index=False, index_label=False)
###############################
        name=['订单编号','运费收入','币种','账务时间']
        df_transfeeincome = pd.read_excel(path,sheet_name='运费收入', usecols='A:D', names=name)
        df_transfeeincome['area'] = attrjson['area']
        df_transfeeincome['country'] = attrjson['country']
        df_transfeeincome['store'] = attrjson['store']
        df_transfeeincome['week'] = attrjson['week']
        df_transfeeincome['qijian']=attrjson['qijian']
        df_transfeeincome['batchid']=batchid

        df_transfeeincome=df_transfeeincome[['area','country','store','week','qijian',
                                       '订单编号','运费收入','币种','账务时间',
                                       "batchid"
        ]]
        df_transfeeincome.to_sql('newchannel_temu_transfeeincome', con=engine, if_exists='append', index=False, index_label=False)
###############################
        name=['订单编号','运费退款','币种','账务时间']
        df_transfeerefund = pd.read_excel(path,sheet_name='运费退款', usecols='A:D', names=name)
        df_transfeerefund['area'] = attrjson['area']
        df_transfeerefund['country'] = attrjson['country']
        df_transfeerefund['store'] = attrjson['store']
        df_transfeerefund['week'] = attrjson['week']
        df_transfeerefund['qijian']=attrjson['qijian']
        df_transfeerefund['batchid']=batchid

        df_transfeerefund=df_transfeerefund[['area','country','store','week','qijian',
                                       '订单编号','运费退款','币种','账务时间',
                                       "batchid"
        ]]
        df_transfeerefund.to_sql('newchannel_temu_transfeerefund', con=engine, if_exists='append', index=False, index_label=False)


        updatebatch(attrjson,batchid,path)

        return 1,''
    except:
        return 2,traceback.format_exc()

if __name__ == '__main__':
    # attrjson={
    #     'area':'us',
    #     'country':'us',
    #     'store':'cd-3',
    #     'week':20
    # }
    # dealsinglefile('E:\pythonws\pythonws\pythonws\playwrighttest\data_proceed\csvs\wfremittance\Wayfair_Remittance_4640701.xlsx',attrjson)
    # # dealsinglefile('E:\pythonws\pythonws\pythonws\playwrighttest\data_proceed\csvs\cdpaymentdetail\\NSD-payment_details_export_139494.xlsx',attrjson)

    # attrjson={
    #     'area': 'eu',
    #     'country':'fr',
    #     'store':'cd-3',
    #     'week':20
    #
    # }
    # dealsinglefile(
    # 'E:\pythonws\pythonws\pythonws\playwrighttest\data_proceed\csvs\mm_\manomano  (3.1-3.31).xlsx',attrjson)
    #

    pass

