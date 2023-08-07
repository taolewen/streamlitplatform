
#cd
import datetime
import os
import traceback
import uuid
from datetime import timedelta
from urllib.parse import quote_plus

import chardet
import pymysql
from dateutil import parser
import pandas as pd

# 显示所有列
from sqlalchemy import create_engine


connstr = "mysql+pymysql://developer:%s@124.71.174.53:3306/csbd?charset=utf8" % quote_plus('csbd@123')
engine = create_engine(connstr)
pd.set_option('display.max_columns', None)
reporttype='dataextract_shopify_order'

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
        sql0 = f"""delete from  newchannel_shopify_order where batchid = '{batchid}' """
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

        # df = pd.read_csv(path)

        try:
            df = pd.read_csv(path,encoding_errors='replace')
        except:
            try:
                f = open(path, 'rb')
                r = f.read()
                f_charinfo = chardet.detect(r)
                print(f_charinfo)
            except:
                print('chardet解析文件编码失败>>'+path)
                traceback.print_exc()
                return 0, traceback.format_exc()

            df = pd.read_csv(path, encoding=f_charinfo["encoding"])



        df.rename(columns={
            "Name": "Name",
            "Email": "Email",
            "Financial Status": "Financial_Status",
            "Paid at": "Paid_at",
            "Fulfillment Status": "Fulfillment_Status",
            "Fulfilled at": "Fulfilled_at",
            "Accepts Marketing": "Accepts_Marketing",
            "Currency": "Currency",
            "Subtotal": "Subtotal",
            "Shipping": "Shipping",
            "Taxes": "Taxes",
            "Total": "Total",
            "Discount Code": "Discount_Code",
            "Discount Amount": "Discount_Amount",
            "Shipping Method": "Shipping_Method",
            "Created at": "Created_at",
            "Lineitem quantity": "Lineitem_quantity",
            "Lineitem name": "Lineitem_name",
            "Lineitem price": "Lineitem_price",
            "Lineitem compare at price": "Lineitem_compare_at_price",
            "Lineitem sku": "Lineitem_sku",
            "Lineitem requires shipping": "Lineitem_requires_shipping",
            "Lineitem taxable": "Lineitem_taxable",
            "Lineitem fulfillment status": "Lineitem_fulfillment_status",
            "Billing Name": "Billing_Name",
            "Billing Street": "Billing_Street",
            "Billing Address1": "Billing_Address1",
            "Billing Address2": "Billing_Address2",
            "Billing Company": "Billing_Company",
            "Billing City": "Billing_City",
            "Billing Zip": "Billing_Zip",
            "Billing Province": "Billing_Province",
            "Billing Country": "Billing_Country",
            "Billing Phone": "Billing_Phone",
            "Shipping Name": "Shipping_Name",
            "Shipping Street": "Shipping_Street",
            "Shipping Address1": "Shipping_Address1",
            "Shipping Address2": "Shipping_Address2",
            "Shipping Company": "Shipping_Company",
            "Shipping City": "Shipping_City",
            "Shipping Zip": "Shipping_Zip",
            "Shipping Province": "Shipping_Province",
            "Shipping Country": "Shipping_Country",
            "Shipping Phone": "Shipping_Phone",
            "Notes": "Notes",
            "Note Attributes": "Note_Attributes",
            "Cancelled at": "Cancelled_at",
            "Payment Method": "Payment_Method",
            "Payment Reference": "Payment_Reference",
            "Refunded Amount": "Refunded_Amount",
            "Vendor": "Vendor",
            "Outstanding Balance": "Outstanding_Balance",
            "Employee": "Employee",
            "Location": "Location",
            "Device ID": "Device_ID",
            "Id": "Id",
            "Tags": "Tags",
            "Risk Level": "Risk_Level",
            "Source": "Source",
            "Lineitem discount": "Lineitem_discount",
            "Tax 1 Name": "Tax_1_Name",
            "Tax 1 Value": "Tax_1_Value",
            "Tax 2 Name": "Tax_2_Name",
            "Tax 2 Value": "Tax_2_Value",
            "Tax 3 Name": "Tax_3_Name",
            "Tax 3 Value": "Tax_3_Value",
            "Tax 4 Name": "Tax_4_Name",
            "Tax 4 Value": "Tax_4_Value",
            "Tax 5 Name": "Tax_5_Name",
            "Tax 5 Value": "Tax_5_Value",
            "Phone": "Phone",
            "Receipt Number": "Receipt_Number",
            "Duties": "Duties",
            "Billing Province Name": "Billing_Province_Name",
            "Shipping Province Name": "Shipping_Province_Name",
            "Payment ID": "Payment_ID",
            "Payment Terms Name": "Payment_Terms_Name",
            "Next Payment Due At": "Next_Payment_Due_At"

        },inplace=True)
        df['area'] = attrjson['area']
        df['country'] = attrjson['country']
        df['store'] = attrjson['store']
        df['week'] = attrjson['week']
        df['qijian']=attrjson['qijian']
        df['batchid']=batchid

        df=df[['area','country','store','week','qijian',
               "Name",
               "Email",
               "Financial_Status",
               "Paid_at",
               "Fulfillment_Status",
               "Fulfilled_at",
               "Accepts_Marketing",
               "Currency",
               "Subtotal",
               "Shipping",
               "Taxes",
               "Total",
               "Discount_Code",
               "Discount_Amount",
               "Shipping_Method",
               "Created_at",
               "Lineitem_quantity",
               "Lineitem_name",
               "Lineitem_price",
               "Lineitem_compare_at_price",
               "Lineitem_sku",
               "Lineitem_requires_shipping",
               "Lineitem_taxable",
               "Lineitem_fulfillment_status",
               "Notes",
               "Note_Attributes",
               "Cancelled_at",
               "Payment_Method",
               "Payment_Reference",
               "Refunded_Amount",
               "Vendor",
               # "Outstanding_Balance",
               # "Employee",
               # "Location",
               # "Device_ID",
               "Id",
               # "Tags",
               "Risk_Level",
               "Source",
               "Lineitem_discount",
               "Tax_1_Name",
               "Tax_1_Value",
               "Tax_2_Name",
               "Tax_2_Value",
               "Tax_3_Name",
               "Tax_3_Value",
               "Tax_4_Name",
               "Tax_4_Value",
               "Tax_5_Name",
               "Tax_5_Value",
               "Receipt_Number",
               "Duties",
               "Billing_Province_Name",
               "Shipping_Province_Name",
               "Payment_ID",
               "Payment_Terms_Name",
               "Next_Payment_Due_At",
               "batchid"
        ]]

        df.to_sql('newchannel_shopify_order', con=engine, if_exists='append', index=False, index_label=False)
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



