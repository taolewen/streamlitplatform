# cd
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
reporttype='dataextract_os_orders'

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
        sql0 = f"""delete from  newchannel_os_orders where batchid = '{batchid}' """
        cursor.execute(sql0)
        sql = f"""delete from  newchannel_batchinfo where batchid = '{batchid}' """
        cursor.execute(sql)
        conn.commit()
        cursor.close()
        conn.close()
        return 1,''
    except:
        return 2,traceback.format_exc()
def dealsinglefile(path, attrjson):
    try:
        batchid=getuid()
        name=[
            'Retailer_Name',
            'Retailer_Order_Number',
            'Warehouse_Name',
            'SOFS_Order_Number',
            'Order_Date',
            'Created_Date',
            'Allow_Partial_Fulfillment',
            'Status',
            'Retailer_Code',
            'Retailer_Order_Code',
            'Backorder_Date',
            'Action_Required',
            'Ship_Contact_Name',
            'Ship_Address_1',
            'Ship_Address_2',
            'Ship_Address_3',
            'Ship_City',
            'Ship_State_Or_Province',
            'Ship_Postal_Code',
            'Ship_Country_Code',
            'Ship_Phone',
            'Ship_Alternate_Phone',
            'Return_Contact_Name',
            'Return_Address_1',
            'Return_Address_2',
            'Return_Address_3',
            'Return_City',
            'Return_State_Or_Province',
            'Return_Postal_Code',
            'Return_Country_Code',
            'Return_Phone',
            'Return_Alternate_Phone',
            'Is_Third_Party_Billing',
            'Is_Signature_Required',
            'Is_Declared_Value_Required',
            'International_Shipment_Terms',
            'Is_International',
            'Ltl_Service_Level',
            'Carrier_Notes',
            'Ltl_Carrier',
            'Freight_Payor_Contact_Name',
            'Freight_Payor_Address_1',
            'Freight_Payor_Address_2',
            'Freight_Payor_Address_3',
            'Freight_Payor_City',
            'Freight_Payor_State_Or_Province',
            'Freight_Payor_Postal_Code',
            'Freight_Payor_Country_Code',
            'Freight_Payor_Phone',
            'Freight_Payor_Alternate_Phone',
            'Shipping_Service_Level_Small_Parcel',
            'Shipping_Specifications_Billing_Account_Number',
            'Shipper_Of_Record_Account_Number',
            'Small_Parcel_Carrier',
            'Usps_Mailer_Id',
            'Light_Weight_0_to_16_Oz_Shipping_Service_Level',
            'Light_Weight_0_to_16_Oz_Shipper_Of_Record_Account_Number',
            'Light_Weight_0_to_16_Oz_Carrier',
            'Light_Weight_0_to_16_Oz_Usps_Mailer_Id',
            'Light_Weight_1_lb_Shipping_Service_Level',
            'Light_Weight_1_lb_Shipper_Of_Record_Account_Number',
            'Light_Weight_1_lb_Carrier',
            'Light_Weight_1_lb_Usps_Mailer_Id',
            'Packing_Slip_Promo_Text',
            'Return_Policy_Text',
            'Brand',
            'Supplier_SKU',
            'SOFS_Order_Line_Number',
            'Quantity',
            'Barcode',
            'SOFS_SKU',
            'Special_Handling',
            'Gift_Wrap_Text',
            'Item_Price',
            'Item_Name',
            'UPC',
            'Line_Status',
            'Cancel_Reason',
            'Unit_Cost',
            'Retailer_First_Cost',
            'Retailer_Additional_Shipping_Cost',
            'Quantity_Shipped',
            'Package_Type',
            'Package_Number',
            'Package_Weight',
            'Package_Weight_Unit_Of_Measure',
            'Tracking_Number',
            'Supplier_Invoice_Number',
            'Carrier_Code',
            'Bill_Of_Lading_Number',
            'Ship_Confirmation_Billing_Account_Number',
            'Service_Level',
            'Carrier_Reference_Number',
            'Date_Shipped',
            'Date_Confirmed',
            'Warehouse_Shipped_From'

        ]
        df = pd.read_csv(path,names=name,skiprows=1)

        df['area'] = attrjson['area']
        df['country'] = attrjson['country']
        df['store'] = attrjson['store']
        df['week'] = attrjson['week']
        df['qijian']=attrjson['qijian']
        df['batchid']=batchid

        df=df[[
            'area', 'country', 'store', 'week', 'qijian',
            'Retailer_Name',
            'Retailer_Order_Number',
            'Warehouse_Name',
            'SOFS_Order_Number',
            'Order_Date',
            'Created_Date',
            'Allow_Partial_Fulfillment',
            'Status',
            'Retailer_Code',
            'Retailer_Order_Code',
            'Backorder_Date',
            'Action_Required',
            'Ship_Contact_Name',
            'Ship_Address_1',
            'Ship_Address_2',
            'Ship_Address_3',
            'Ship_City',
            'Ship_State_Or_Province',
            'Ship_Postal_Code',
            'Ship_Country_Code',
            'Ship_Phone',
            'Ship_Alternate_Phone',
            'Return_Contact_Name',
            'Return_Address_1',
            'Return_Address_2',
            'Return_Address_3',
            'Return_City',
            'Return_State_Or_Province',
            'Return_Postal_Code',
            'Return_Country_Code',
            'Return_Phone',
            'Return_Alternate_Phone',
            'Is_Third_Party_Billing',
            'Is_Signature_Required',
            'Is_Declared_Value_Required',
            'International_Shipment_Terms',
            'Is_International',
            'Ltl_Service_Level',
            'Carrier_Notes',
            'Ltl_Carrier',
            'Freight_Payor_Contact_Name',
            'Freight_Payor_Address_1',
            'Freight_Payor_Address_2',
            'Freight_Payor_Address_3',
            'Freight_Payor_City',
            'Freight_Payor_State_Or_Province',
            'Freight_Payor_Postal_Code',
            'Freight_Payor_Country_Code',
            'Freight_Payor_Phone',
            'Freight_Payor_Alternate_Phone',
            'Shipping_Service_Level_Small_Parcel',
            'Shipping_Specifications_Billing_Account_Number',
            'Shipper_Of_Record_Account_Number',
            'Small_Parcel_Carrier',
            'Usps_Mailer_Id',
            'Light_Weight_0_to_16_Oz_Shipping_Service_Level',
            'Light_Weight_0_to_16_Oz_Shipper_Of_Record_Account_Number',
            'Light_Weight_0_to_16_Oz_Carrier',
            'Light_Weight_0_to_16_Oz_Usps_Mailer_Id',
            'Light_Weight_1_lb_Shipping_Service_Level',
            'Light_Weight_1_lb_Shipper_Of_Record_Account_Number',
            'Light_Weight_1_lb_Carrier',
            'Light_Weight_1_lb_Usps_Mailer_Id',
            'Packing_Slip_Promo_Text',
            'Return_Policy_Text',
            'Brand',
            'Supplier_SKU',
            'SOFS_Order_Line_Number',
            'Quantity',
            'Barcode',
            'SOFS_SKU',
            'Special_Handling',
            'Gift_Wrap_Text',
            'Item_Price',
            'Item_Name',
            'UPC',
            'Line_Status',
            'Cancel_Reason',
            'Unit_Cost',
            'Retailer_First_Cost',
            'Retailer_Additional_Shipping_Cost',
            'Quantity_Shipped',
            'Package_Type',
            'Package_Number',
            'Package_Weight',
            'Package_Weight_Unit_Of_Measure',
            'Tracking_Number',
            'Supplier_Invoice_Number',
            'Carrier_Code',
            'Bill_Of_Lading_Number',
            'Ship_Confirmation_Billing_Account_Number',
            'Service_Level',
            'Carrier_Reference_Number',
            'Date_Shipped',
            'Date_Confirmed',
            'Warehouse_Shipped_From',
            'batchid'
        ]]

        df.to_sql('newchannel_os_orders', con=engine, if_exists='append', index=False, index_label=False)
        updatebatch(attrjson,batchid,path)
        return 1,''
    except:
        return 2,traceback.format_exc()




if __name__ == '__main__':
    dealsinglefile('C:\\Users\维1-26\Desktop\os-OrdersReport-DE(6).csv','')


