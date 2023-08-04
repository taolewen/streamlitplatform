
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
reporttype='dataextract_wf_logisticinvoice'

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
        sql0 = f"""delete from  newchannel_wf_logisticinvoice where batchid = '{batchid}' """
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

        names=["Invoice_#",
                "Charge_#",
                "Charge_Date",
                "Event_Date",
                "Charge_Category",
                "Type",
                "Order_#",
                "Retailer_Name",
                "Adjustment_Description",
                "SPO_#",
                "Part_Number",
                "Product_Bucket",
                "Bin_Units_Per_Order",
                "Dimensions_L",
                "Dimensions_W",
                "Dimensions_H",
                "Weight",
                "Girth",
                "Carton_Number",
                "Charge_Units",
                "Metric_Quantity",
                "Metric",
                "Local_Currency",
                "Charge_Amount",
                "Original_Currency",
                "Original_Charge_Amount",
                "Unit_Wholesale_Cost_USD",
                "Exchange_Rate",
                "Warehouse_Wholesale_Cost_USD",
                "Total_Wholesale_Cost_USD",
                "Ship_Speed",
                "Batch_Number",
                "Warehouse",
                "Admin_Description_Notes"]
        df=pd.read_csv(path,skiprows=1,names=names)

        df['area']=attrjson['area']
        df['country']=attrjson['country']
        df['store']=attrjson['store']
        df['week']=attrjson['week']
        df['qijian'] = attrjson['qijian']
        df['batchid']=batchid



        df=df[['area','country','store','week','qijian',
               "Invoice_#",
                "Charge_#",
                "Charge_Date",
                "Event_Date",
                "Charge_Category",
                "Type",
                "Order_#",
                "Retailer_Name",
                "Adjustment_Description",
                "SPO_#",
                "Part_Number",
                "Product_Bucket",
                "Bin_Units_Per_Order",
                "Dimensions_L",
                "Dimensions_W",
                "Dimensions_H",
                "Weight",
                "Girth",
                "Carton_Number",
                "Charge_Units",
                "Metric_Quantity",
                "Metric",
                "Local_Currency",
                "Charge_Amount",
                "Original_Currency",
                "Original_Charge_Amount",
                "Unit_Wholesale_Cost_USD",
                "Exchange_Rate",
                "Warehouse_Wholesale_Cost_USD",
                "Total_Wholesale_Cost_USD",
                "Ship_Speed",
                "Batch_Number",
                "Warehouse",
                "Admin_Description_Notes","batchid"]]



        print(df)

        df.to_sql('newchannel_wf_logisticinvoice', con=engine, if_exists='append', index=False, index_label=False)
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
        'qijian':1234,
        'importdate':'2022-03-06'

    }
    dealsinglefile('C:\\Users\\Administrator\\Desktop\\小平台取数逻辑表\\WF\\Invoice_213614_20230406043244.csv', attrjson)
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
