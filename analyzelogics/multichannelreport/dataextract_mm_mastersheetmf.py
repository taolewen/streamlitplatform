
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
reporttype='dataextract_mm_mastersheetmf'

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
        sql0 = f"""delete from  newchannel_mm_mastersheetmf where batchid = '{batchid}' """
        cursor.execute(sql0)
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
        name=['SELLER_ACCOUNT_ID',
            'SELLER_ID',
            'SELLER_NAME',
            'PLATFORM',
            'INVOICING_ITEM',
            'SUBCATEGORY',
            'SHIPMENT_NUMBER',
            'IN_STOCK_DATE',
            'INBOUND_WH',
            'EAN',
            'SELLER_SKU',
            'MM_ID',
            'PRODUCT_PRICE_VAT_EXC',
            'PRODUCT_NAME',
            'IS_MULTIPART',
            'LENGTH_CM',
            'WIDTH_CM',
            'HEIGHT_CM',
            'WEIGHT_KG',
            'VOLUME_M3',
            'ORDER_REFERENCE',
            'QUANTITY_ORDERED',
            'ORDER_DATE',
            'DISPATCH_DATE',
            'DISPATCH_WAREHOUSE',
            'DESTINATION_PLATFORM',
            'REFUND_DATE',
            'RETURN_STATUS',
            'RETURN_REASON',
            'CUSTOMER_REFUND_AMOUNT',
            'COMPENSATION_REASON',
            'COMPENSATION_TYPE',
            'PRODUCT_COMPENSATION',
            'APPLICABLE_DISPATCH_FEE',
            'UNIT_PRICE_VAT_EXC',
            'QUANTITY',
            'QUANTITY_TYPE',
            'GROSS_AMOUNT_VAT_EXC',
            'NET_AMOUNT_VAT_EXC'
              ]
        # df=pd.read_excel(path,sheet_name='soges MF-退款')
        # df = pd.read_excel(path, names = name)
        df = pd.read_excel(path, names = name,skiprows=1)

        # df=df.iloc[:,0:21]
        # df.to_csv('fdfd0.csv')
        # print(df)
        df['area'] = attrjson['area']
        df['country'] = attrjson['country']
        df['store'] = attrjson['store']
        df['week'] = attrjson['week']
        df['qijian']=attrjson['qijian']
        df['batchid']=batchid
        # df.to_csv('fdfd.csv')

        df=df[['area','country','store','week','qijian',
               'SELLER_ACCOUNT_ID',
                'SELLER_ID',
                'SELLER_NAME',
                'PLATFORM',
                'INVOICING_ITEM',
                'SUBCATEGORY',
                'SHIPMENT_NUMBER',
                'IN_STOCK_DATE',
                'INBOUND_WH',
                'EAN',
                'SELLER_SKU',
                'MM_ID',
                'PRODUCT_PRICE_VAT_EXC',
                'PRODUCT_NAME',
                'IS_MULTIPART',
                'LENGTH_CM',
                'WIDTH_CM',
                'HEIGHT_CM',
                'WEIGHT_KG',
                'VOLUME_M3',
                'ORDER_REFERENCE',
                'QUANTITY_ORDERED',
                'ORDER_DATE',
                'DISPATCH_DATE',
                'DISPATCH_WAREHOUSE',
                'DESTINATION_PLATFORM',
                'REFUND_DATE',
                'RETURN_STATUS',
                'RETURN_REASON',
                'CUSTOMER_REFUND_AMOUNT',
                'COMPENSATION_REASON',
                'COMPENSATION_TYPE',
                'PRODUCT_COMPENSATION',
                'APPLICABLE_DISPATCH_FEE',
                'UNIT_PRICE_VAT_EXC',
                'QUANTITY',
                'QUANTITY_TYPE',
                'GROSS_AMOUNT_VAT_EXC',
                'NET_AMOUNT_VAT_EXC',
               "batchid"
        ]]

        df.to_sql('newchannel_mm_mastersheetmf', con=engine, if_exists='append', index=False, index_label=False)
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



    attrjson={
        'area': 'eu',
        'country':'fr',
        'store':'mm-3',
        'week':20,
        'qijian':'22332'

    }
    print(
    dealsinglefile(
    'D:\pythonws\pythonws\playwrighttest\data_proceed\csvs\mm_\新建 Microsoft Excel 工作表.xlsx',attrjson)
    )
    #



