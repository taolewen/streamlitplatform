
#cd
import datetime
import os
import traceback
import uuid
from datetime import timedelta
from urllib.parse import quote_plus
import openpyxl
import pymysql
from dateutil import parser
import pandas as pd

# 显示所有列
from openpyxl import load_workbook
from sqlalchemy import create_engine


import streamlit as st
host = st.secrets["mysql"]['host'],
user = st.secrets["mysql"]['user'],
password = st.secrets["mysql"]['password'],
db = st.secrets["mysql"]['database']
connstr = f"mysql+pymysql://{user[0]}:%s@{host[0]}:3306/{db}?charset=utf8" % quote_plus(f'{password[0]}')
engine = create_engine(connstr)
pd.set_option('display.max_columns', None)
reporttype='dataextract_wm_order'

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

        sql1 = f"""delete from  newchannel_wm_order where batchid = '{batchid}' """
        cursor.execute(sql1)
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

        df=pd.read_excel(path)
        df.rename(columns={"PO#":"PO",
    "Order#":"Order",
    "Order Date":"Order_Date",
    "Ship By":"Ship_By",
    "Delivery Date":"Delivery_Date",
    "Customer Name":"Customer_Name",
    "Customer Shipping Address":"Customer_Shipping_Address",
    "Customer Phone Number":"Customer_Phone_Number",
    "Ship to Address 1":"Ship_to_Address_1",
    "Ship to Address 2":"Ship_to_Address_2",
    "City":"City",
    "State":"State",
    "Zip":"Zip",
    "Segment":"Segment",
    "FLIDS":"FLIDS",
    "Line#":"Line",
    "UPC":"UPC",
    "Status":"Status",
    "Item Description":"Item_Description",
    "Shipping Method":"Shipping_Method",
    "Shipping Tier":"Shipping_Tier",
    "Shipping SLA":"Shipping_SLA",
    "Shipping Config SOurce":"Shipping_Config_SOurce",
    "Qty":"Qty",
    "SKU":"SKU",
    "Item Cost":"Item_Cost",
    "Shipping Cost":"Shipping_Cost",
    "Tax":"Tax",
    "Update Status":"Update_Status",
    "Update Qty":"Update_Qty",
    "Carrier":"Carrier",
    "Tracking Number":"Tracking_Number",
    "Tracking Url":"Tracking_Url",
    "Seller Order NO":"Seller_Order_NO",
    "Fulfillment Entity":"Fulfillment_Entity",
    "Replacement Order":"Replacement_Order",
    "Original Customer Order Id":"Original_Customer_Order_Id"},inplace=True)
        df['area'] = attrjson['area']
        df['country'] = attrjson['country']
        df['store'] = attrjson['store']
        df['week'] = attrjson['week']
        df['qijian'] = attrjson['qijian']

        df=df[['area','country','store','week','qijian',
            "PO",
            "Order",
            "Order_Date",
            "Ship_By",
            "Delivery_Date",
            "Customer_Name",
            "Customer_Shipping_Address",
            "Customer_Phone_Number",
            "Ship_to_Address_1",
            "Ship_to_Address_2",
            "City",
            "State",
            "Zip",
            "Segment",
            "FLIDS",
            "Line",
            "UPC",
            "Status",
            "Item_Description",
            "Shipping_Method",
            "Shipping_Tier",
            "Shipping_SLA",
            "Shipping_Config_SOurce",
            "Qty",
            "SKU",
            "Item_Cost",
            "Shipping_Cost",
            "Tax",
            "Update_Status",
            "Update_Qty",
            "Carrier",
            "Tracking_Number",
            "Tracking_Url",
            "Seller_Order_NO",
            "Fulfillment_Entity",
            "Replacement_Order",
            "Original_Customer_Order_Id"
        ]]
        df['batchid']=batchid

        df.to_sql('newchannel_wm_order', con=engine, if_exists='append', index=False, index_label=False)
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
    'E:\pythonws\pythonws\pythonws\playwrighttest\data_proceed\csvs\wm_订单\PO_Data_2022-04-01_00_41_37PST.xlsx',attrjson)
    #



