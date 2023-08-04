
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


connstr = "mysql+pymysql://developer:%s@124.71.174.53:3306/csbd?charset=utf8" % quote_plus('csbd@123')
engine = create_engine(connstr)
pd.set_option('display.max_columns', None)
reporttype='dataextract_wf_order_shipped'

def getuid():
    uid = str(uuid.uuid4())
    suid = ''.join(uid.split('-'))
    return suid
def updatebatch(attrjson,batchid,path):
    conn = pymysql.connect(host='124.71.174.53',
                           user='developer',
                           password='csbd@123',
                           database='csbd')
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
    return strlist
def deletebatch(batchid):
    try:
        conn = pymysql.connect(host='124.71.174.53',
                               user='developer',
                               password='csbd@123',
                               database='csbd')
        cursor = conn.cursor()
        sql0 = f"""delete from  newchannel_wf_ordershipped where batchid = '{batchid}' """
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

        names=["Warehouse_Name","Store_Name","PO_Number","PO_Date","Must_Ship_By","Backorder_Date",
               "Order_Status","Item_Number","Item_Name","Quantity","Wholesale_Price","Ship_Method",
               "Carrier_Name","Shipping_Account_Number","Ship_To_Name","Ship_To_Address","Ship_To_Address_2","Ship_To_City",
               "Ship_To_State","Ship_To_Zip","Ship_To_Phone","Inventory_at_PO_Time","Inventory_Send_Date","Ship_Speed",
               "PO_Date_&_Time","Registered_Timestamp","Customization_Text","Event_Name","Event_ID","Event_Start_Date",
               "Event_End_Date","Event_Type","Backorder_Reason","Original_Product_ID","Original_Product_Name","Event_Inventory_Source",
               "Packing_Slip_URL","Tracking_Number","Ready_for_Pickup_Date","SKU","Destination_Country","Depot_ID",
               "Depot_Name","Wholesale_Event_Source","Wholesale_Event_Store_Source","B2BOrder","Composite_Wood_Product","Sales_Channel"]
        df=pd.read_csv(path,skiprows=1,names=names)

        df['area']=attrjson['area']
        df['country']=attrjson['country']
        df['store']=attrjson['store']
        df['week']=attrjson['week']
        df['qijian'] = attrjson['qijian']
        df['batchid']=batchid
        print(df.info())

        print(df['Item_Number'])

        df['Item_Number'] = df['Item_Number'].astype(str)
        df['Item_Number'] = df['Item_Number'].apply(
            lambda x: x.replace('=', '').replace('"', ''))

        df=df[['area','country','store','week','qijian',
               "Warehouse_Name","Store_Name","PO_Number","PO_Date","Must_Ship_By","Backorder_Date",
               "Order_Status","Item_Number","Item_Name","Quantity","Wholesale_Price","Ship_Method",
               "Carrier_Name","Shipping_Account_Number","Ship_To_Name","Ship_To_Address","Ship_To_Address_2","Ship_To_City",
               "Ship_To_State","Ship_To_Zip","Ship_To_Phone","Inventory_at_PO_Time","Inventory_Send_Date","Ship_Speed",
               "PO_Date_&_Time","Registered_Timestamp","Customization_Text","Event_Name","Event_ID","Event_Start_Date",
               "Event_End_Date","Event_Type","Backorder_Reason","Original_Product_ID","Original_Product_Name","Event_Inventory_Source",
               "Packing_Slip_URL","Tracking_Number","Ready_for_Pickup_Date","SKU","Destination_Country","Depot_ID",
               "Depot_Name","Wholesale_Event_Source","Wholesale_Event_Store_Source","B2BOrder","Composite_Wood_Product","Sales_Channel","batchid"]]



        print(df)

        df.to_sql('newchannel_wf_ordershipped', con=engine, if_exists='append', index=False, index_label=False)
        updatebatch(attrjson,batchid,path)

        return 1,''
    except:
        return 2,traceback.format_exc()


if __name__ == '__main__':
    attrjson={
        'area': 'us',
        'country':'',
        'store':'WF-US-1',
        'week':20,
        'importdate':'2022-03-06'

    }
    dealsinglefile('/data_proceed/csvs/wf已发货/us-已发货订单-0301-0306.csv', attrjson)
    # # dealsinglefile('E:\pythonws\pythonws\pythonws\playwrighttest\data_proceed\csvs\cdpaymentdetail\\NSD-payment_details_export_139494.xlsx',attrjson)

    # attrjson={
    #     'area': 'eu',
    #     'country':'',
    #     'store':'WF-EU-5',
    #     'week':20,
    #     'importdate':'2022-03-20'
    #
    # }
    # dealsinglefile(
    #     'D:\pythonws\pythonws\playwrighttest\data_proceed\csvs\wf已发货\eu-已发货订单.csv', attrjson)
    #
