
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


connstr = "mysql+pymysql://developer:%s@124.71.174.53:3306/csbd?charset=utf8" % quote_plus('csbd@123')
engine = create_engine(connstr)
pd.set_option('display.max_columns', None)
reporttype='dataextract_wm_return'

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
    df=df.drop('path', axis=1)
    df['delete']=False
    df=df[['delete','area','country','qijian','week','store','createdate','reporttype','batchid']]
    return df
def deletebatch(batchid):
    try:
        conn = pymysql.connect(host='124.71.174.53',
                               user='developer',
                               password='csbd@123',
                               database='csbd')
        cursor = conn.cursor()

        sql1 = f"""delete from  newchannel_wm_return where batchid = '{batchid}' """
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
        #原表头
    #     df.rename(columns={"Customer Order #":"Customer_Order",
    # "PO#":"PO",
    # "PO Line #":"PO_Line",
    # "RMA#":"RMA",
    # "Return Order Line#":"Return_Order_Line",
    # "Request Date":"Request_Date",
    # "Return Reason":"Return_Reason",
    # "Item ID":"Item_ID",
    # "Current Status":"Current_Status",
    # "Current Status Time":"Current_Status_Time",
    # "Keep It":"Keep_It",
    # "Item Name":"Item_Name",
    # "Carrier":"Carrier",
    # "Tracking Number":"Tracking_Number",
    # "Channel":"Channel",
    # "Customer Name":"Customer_Name",
    # "Customer Email":"Customer_Email",
    # "Customer Comments":"Customer_Comments",
    # "Order Amount":"Order_Amount",
    # "Return Shipping Fee":"Return_Shipping_Fee",
    # "Restocking Fee":"Restocking_Fee",
    # "Total Refund Amount":"Total_Refund_Amount",
    # "Seller Order No":"Seller_Order_No",
    # "Refund Mode":"Refund_Mode",
    # "Refund Channel":"Refund_Channel",
    # "Tracking Status ":"Tracking_Status",
    # "Refund Status":"Refund_Status",
    # "Replacement":"Replacement"},inplace=True)
        #新表头
        df.rename(columns={"CUSTOMER_ORDER_NO": "Customer_Order",
                           "PO_NO": "PO",
                           "PO_LINE_NO": "PO_Line",
                           "RMA_NO": "RMA",
                           "RETURN_ORDER_LINE_NO": "Return_Order_Line",
                           "REQUEST_DATE": "Request_Date",
                           "RETURN_REASON": "Return_Reason",
                           "ITEM_ID": "Item_ID",
                           "CURRENT_STATUS": "Current_Status",
                           "CURRENT_STATUS_TIME": "Current_Status_Time",
                           "KEEP_IT": "Keep_It",
                           "ITEM_NAME": "Item_Name",
                           "CARRIER": "Carrier",
                           "TRACKING_NUMBER": "Tracking_Number",
                           "CHANNEL": "Channel",
                           "CUSTOMER_NAME": "Customer_Name",
                           "CUSTOMER_EMAIL": "Customer_Email",
                           "CUSTOMER_COMMENTS": "Customer_Comments",
                           "ORDER_AMOUNT": "Order_Amount",
                           "RETURN_SHIPPING_FEE": "Return_Shipping_Fee",
                           "RESTOCKING_FEE": "Restocking_Fee",
                           "TOTAL_REFUND_AMOUNT": "Total_Refund_Amount",
                           "SELLER_ORDER_NO": "Seller_Order_No",
                           "REFUND_MODE": "Refund_Mode",
                           "REFUND_CHANNEL": "Refund_Channel",
                           "TRACKING_STATUS": "Tracking_Status",
                           "REFUND_STATUS": "Refund_Status",
                           "REPLACEMENT": "Replacement"}, inplace=True)
        df['area'] = attrjson['area']
        df['country'] = attrjson['country']
        df['store'] = attrjson['store']
        df['week'] = attrjson['week']
        df['qijian'] = attrjson['qijian']

        df=df[['area','country','store','week','qijian',
               "Customer_Order",
               "PO",
               "PO_Line",
               "RMA",
               "Return_Order_Line",
               "Request_Date",
               "Return_Reason",
               "Item_ID",
               "Current_Status",
               "Current_Status_Time",
               "Keep_It",
               "Item_Name",
               "Carrier",
               "Tracking_Number",
               "Channel",
               "Customer_Name",
               "Customer_Email",
               "Customer_Comments",
               "Order_Amount",
               "Return_Shipping_Fee",
               "Restocking_Fee",
               "Total_Refund_Amount",
               "Seller_Order_No",
               "Refund_Mode",
               "Refund_Channel",
               "Tracking_Status",
               "Refund_Status",
               "Replacement"
        ]]
        df['batchid']=batchid

        df.to_sql('newchannel_wm_return', con=engine, if_exists='append', index=False, index_label=False)
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
    'E:\pythonws\pythonws\pythonws\playwrighttest\data_proceed\csvs\wm_退货\Return_Orders_2022-03-31_23_42_51PST.xlsx',attrjson)
    #



