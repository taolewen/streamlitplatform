
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
reporttype='dataextract_mm_payment'

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
    df=df.drop('path', axis=1)
    df['delete']=False
    df=df[['delete','area','country','qijian','week','store','createdate','reporttype','batchid']]
    return df
def deletebatch(batchid):
    try:
        conn = pymysql.connect(host=st.secrets["mysql"]['host'],
                               user=st.secrets["mysql"]['user'],
                               password=st.secrets["mysql"]['password'],
                               db=st.secrets["mysql"]['database'])
        cursor = conn.cursor()
        sql0 = f"""delete from  newchannel_mm_payment where batchid = '{batchid}' """
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
        name=['item_create_time',
            'customer_last_name',
            'customer_first_name',
            'item_ref',
            'item_type',
            'order_product_id',
            'product_name',
            'quantity',
            'installment',
            'amount_vat_incl',
            'amount_vat_excl',
            'products_total',
            'shipping_total',
            'commission_vat_excl',
            'tva_comission',
            'commission_vat_incl',
            'net_amount',
            'coupons',
            'currency',
            'bank_transfer_ref',
            'transfer_date',
            'vat_liability',
            'vat_amount',
            'invoice_issuer',
            'invoice_reference',
              'invoice_number'
              ]
        # df=pd.read_excel(path,sheet_name='soges MF-退款')
        # df = pd.read_excel(path, names = name)
        df = pd.read_csv(path, names = name,skiprows=1)
        df['shop']=attrjson['addcoldict'].get('shop')

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
               'item_create_time',
               'customer_last_name',
               'customer_first_name',
               'item_ref',
               'item_type',
               'order_product_id',
               'product_name',
               'quantity',
               'installment',
               'amount_vat_incl',
               'amount_vat_excl',
               'products_total',
               'shipping_total',
               'commission_vat_excl',
               'tva_comission',
               'commission_vat_incl',
               'net_amount',
               'coupons',
               'currency',
               'bank_transfer_ref',
               'transfer_date',
               'vat_liability',
               'vat_amount',
               'invoice_issuer',
               'shop',
               "batchid"
        ]]

        df.to_sql('newchannel_mm_payment', con=engine, if_exists='append', index=False, index_label=False)
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



