
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
reporttype='dataextract_mm_return'

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
def selectbatch():
    sql = f"""select * from newchannel_batchinfo where reporttype='{reporttype}' order by createdate desc"""
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
    return strlist
def deletebatch(batchid):
    try:
        conn = pymysql.connect(host=st.secrets["mysql"]['host'],
                               user=st.secrets["mysql"]['user'],
                               password=st.secrets["mysql"]['password'],
                               db=st.secrets["mysql"]['database'])
        cursor = conn.cursor()
        sql0 = f"""delete from  newchannel_mm_return where batchid = '{batchid}' """
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

        # df=pd.read_excel(path,sheet_name='soges MF-退款')
        df = pd.read_excel(path)

        df.rename(columns={
            "Date": "Date",
            "Order Reference": "Order_Reference",
            "Email": "Email",
            "Surname": "Surname",
            "Name": "Name",
            "Company": "Company",
            "Address 1": "Address_1",
            "Address 2": "Address_2",
            "Address 3": "Address_3",
            "postal code": "postal_code",
            "City": "City",
            "Country": "Country_1",
            "Country ISO": "Country_ISO",
            "Telephone": "Telephone",
            "Carrier": "Carrier",
            "Relay identifier": "Relay_identifier",
            "Relay name": "Relay_name",
            "Relay address": "Relay_address",
            "Relay postal code": "Relay_postal_code",
            "Relay city": "Relay_city",
            "Relay country": "Relay_country",
            "Relay country ISO": "Relay_country_ISO",
            "Billing surname": "Billing_surname",
            "Billing name": "Billing_name",
            "Billing company": "Billing_company",
            "Billing Address 1": "Billing_Address_1",
            "Billing Address 2": "Billing_Address_2",
            "Billing Address 3": "Billing_Address_3",
            "Billing postal code": "Billing_postal_code",
            "Billing City": "Billing_City",
            "Billing Country": "Billing_Country",
            "Billing Country ISO": "Billing_Country_ISO",
            "Billing telephone": "Billing_telephone",
            "DNI/NIF/CIF/NIE": "DNI_NIF_CIF_NIE",
            "Product": "Product",
            "Product reference": "Product_reference",
            "Quantity": "Quantity",
            "Price VAT excluded": "Price_VAT_excluded",
            "Shipping costs VAT excluded": "Shipping_costs_VAT_excluded",
            "Price VAT included": "Price_VAT_included",
            "Status": "Status",
            "Delivery date": "Delivery_date",
            "Concrete guarantee status": "Concrete_guarantee_status"

        },inplace=True)
        df['area'] = attrjson['area']
        df['country'] = attrjson['country']
        df['store'] = attrjson['store']
        df['week'] = attrjson['week']
        df['qijian']=attrjson['qijian']
        df['batchid']=batchid

        df=df[['area','country','store','week','qijian',
               "Date",
               "Order_Reference",
               "Email",
               "Surname",
               "Name",
               "Company",
               "Address_1",
               "Address_2",
               "Address_3",
               "postal_code",
               "City",
               "Country_1",
               "Country_ISO",
               "Telephone",
               "Carrier",
               "Relay_identifier",
               "Relay_name",
               "Relay_address",
               "Relay_postal_code",
               "Relay_city",
               "Relay_country",
               "Relay_country_ISO",
               "Billing_surname",
               "Billing_name",
               "Billing_company",
               "Billing_Address_1",
               "Billing_Address_2",
               "Billing_Address_3",
               "Billing_postal_code",
               "Billing_City",
               "Billing_Country",
               "Billing_Country_ISO",
               "Billing_telephone",
               "DNI_NIF_CIF_NIE",
               "Product",
               "Product_reference",
               "Quantity",
               "Price_VAT_excluded",
               "Shipping_costs_VAT_excluded",
               "Price_VAT_included",
               "Status",
               "Delivery_date",
               "Concrete_guarantee_status",
               "batchid"
        ]]

        df.to_sql('newchannel_mm_return', con=engine, if_exists='append', index=False, index_label=False)
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
        'store':'cd-3',
        'week':20

    }
    dealsinglefile(
    'E:\pythonws\pythonws\pythonws\playwrighttest\data_proceed\csvs\mm_\manomano  (3.1-3.31).xlsx',attrjson)
    #



