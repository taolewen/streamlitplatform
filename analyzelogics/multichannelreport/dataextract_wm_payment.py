
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
reporttype='dataextract_wm_payment'

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

        sql1 = f"""delete from  newchannel_wm_payment where batchid = '{batchid}' """
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

        df=pd.read_csv(path)
        df.rename(columns={"Walmart.com Order #":"Walmart_com_Order",
    "Walmart.com Order Line #":"Walmart_com_Order_Line",
    "Walmart.com PO #":"Walmart_com_PO",
    "Walmart.com P.O. Line #":"Walmart_com_P_O_Line",
    "Partner Order #":"Partner_Order",
    "Transaction Type":"Transaction_Type",
    "Transaction Date Time":"Transaction_Date_Time",
    "Shipped Qty":"Shipped_Qty",
    "Partner Item ID":"Partner_Item_ID",
    "Partner GTIN":"Partner_GTIN",
    "Partner Item name":"Partner_Item_name",
    "Product tax code":"Product_tax_code",
    "Shipping tax code":"Shipping_tax_code",
    "Gift wrap tax code":"Gift_wrap_tax_code",
    "Ship to state":"Ship_to_state",
    "Ship to county":"Ship_to_county",
    "County Code":"County_Code",
    "Ship to city":"Ship_to_city",
    "Zip code":"Zip_code",
    "shipping_method":"shipping_method",
    "Total tender to / from customer":"Total_tender_to_from_customer",
    "Payable to Partner from Sale":"Payable_to_Partner_from_Sale",
    "Commission from Sale":"Commission_from_Sale",
    "Commission Rate":"Commission_Rate",
    "Gross Sales Revenue":"Gross_Sales_Revenue",
    "Refunded Retail Sales":"Refunded_Retail_Sales",
    "Sales refund for Escalation":"Sales_refund_for_Escalation",
    "Gross Shipping Revenue":"Gross_Shipping_Revenue",
    "Gross Shipping Refunded":"Gross_Shipping_Refunded",
    "Shipping refund for Escalation":"Shipping_refund_for_Escalation",
    "Net Shipping Revenue":"Net_Shipping_Revenue",
    "Gross Fee Revenue":"Gross_Fee_Revenue",
    "Gross Fee Refunded":"Gross_Fee_Refunded",
    "Fee refund for Escalation":"Fee_refund_for_Escalation",
    "Net Fee Revenue":"Net_Fee_Revenue",
    "Gift Wrap Quantity":"Gift_Wrap_Quantity",
    "Gross Gift-Wrap Revenue":"Gross_Gift-Wrap_Revenue",
    "Gross Gift-Wrap Refunded":"Gross_Gift-Wrap_Refunded",
    "Gift wrap refund for Escalation":"Gift_wrap_refund_for_Escalation",
    "Net Gift Wrap Revenue":"Net_Gift_Wrap_Revenue",
    "Tax on Sales Revenue":"Tax_on_Sales_Revenue",
    "Tax on Shipping Revenue":"Tax_on_Shipping_Revenue",
    "Tax on Gift-Wrap Revenue":"Tax_on_Gift-Wrap_Revenue",
    "Tax on Fee Revenue":"Tax_on_Fee_Revenue",
    "Effective tax rate":"Effective_tax_rate",
    "Tax on Refunded Sales":"Tax_on_Refunded_Sales",
    "Tax on Shipping Refund":"Tax_on_Shipping_Refund",
    "Tax on Gift-Wrap Refund":"Tax_on_Gift-Wrap_Refund",
    "Tax on Fee Refund":"Tax_on_Fee_Refund",
    "Tax on Sales refund for Escalation":"Tax_on_Sales_refund_for_Escalation",
    "Tax on Shipping Refund for Escalation":"Tax_on_Shipping_Refund_for_Escalation",
    "Tax on Gift-Wrap Refund for escalation":"Tax_on_Gift-Wrap_Refund_for_escalation",
    "Tax on Fee Refund for escalation":"Tax_on_Fee_Refund_for_escalation",
    "Total NET Tax Collected":"Total_NET_Tax_Collected",
    "Tax Withheld":"Tax_Withheld",
    "Adjustment Description":"Adjustment_Description",
    "Adjustment Code":"Adjustment_Code",
    "Original Item price":"Original_Item_price",
    "Original Commission Amount":"Original_Commission_Amount",
    "Spec Category":"Spec_Category",
    "Contract Category":"Contract_Category",
    "Product Type":"Product_Type",
    "Flex Commission Rule":"Flex_Commission_Rule",
    "Return Reason Code":"Return_Reason_Code",
    "Return Reason Description":"Return_Reason_Description",
    "Fee Withheld Flag":"Fee_Withheld_Flag",
    "Fulfillment Type":"Fulfillment_Type"},inplace=True)
        df['area'] = attrjson['area']
        df['country'] = attrjson['country']
        df['store'] = attrjson['store']
        df['week'] = attrjson['week']
        df['qijian'] = attrjson['qijian']

        df=df[['area','country','store','week','qijian',
            "Walmart_com_Order",
    "Walmart_com_Order_Line",
    "Walmart_com_PO",
    "Walmart_com_P_O_Line",
    "Partner_Order",
    "Transaction_Type",
    "Transaction_Date_Time",
    "Shipped_Qty",
    "Partner_Item_ID",
    "Partner_GTIN",
    "Partner_Item_name",
    "Product_tax_code",
    "Shipping_tax_code",
    "Gift_wrap_tax_code",
    "Ship_to_state",
    "Ship_to_county",
    "County_Code",
    "Ship_to_city",
    "Zip_code",
    "shipping_method",
    "Total_tender_to_from_customer",
    "Payable_to_Partner_from_Sale",
    "Commission_from_Sale",
    "Commission_Rate",
    "Gross_Sales_Revenue",
    "Refunded_Retail_Sales",
    "Sales_refund_for_Escalation",
    "Gross_Shipping_Revenue",
    "Gross_Shipping_Refunded",
    "Shipping_refund_for_Escalation",
    "Net_Shipping_Revenue",
    "Gross_Fee_Revenue",
    "Gross_Fee_Refunded",
    "Fee_refund_for_Escalation",
    "Net_Fee_Revenue",
    "Gift_Wrap_Quantity",
    "Gross_Gift-Wrap_Revenue",
    "Gross_Gift-Wrap_Refunded",
    "Gift_wrap_refund_for_Escalation",
    "Net_Gift_Wrap_Revenue",
    "Tax_on_Sales_Revenue",
    "Tax_on_Shipping_Revenue",
    "Tax_on_Gift-Wrap_Revenue",
    "Tax_on_Fee_Revenue",
    "Effective_tax_rate",
    "Tax_on_Refunded_Sales",
    "Tax_on_Shipping_Refund",
    "Tax_on_Gift-Wrap_Refund",
    "Tax_on_Fee_Refund",
    "Tax_on_Sales_refund_for_Escalation",
    "Tax_on_Shipping_Refund_for_Escalation",
    "Tax_on_Gift-Wrap_Refund_for_escalation",
    "Tax_on_Fee_Refund_for_escalation",
    "Total_NET_Tax_Collected",
    "Tax_Withheld",
    "Adjustment_Description",
    "Adjustment_Code",
    "Original_Item_price",
    "Original_Commission_Amount",
    "Spec_Category",
    "Contract_Category",
    "Product_Type",
    "Flex_Commission_Rule",
    "Return_Reason_Code",
    "Return_Reason_Description",
    "Fee_Withheld_Flag",
    "Fulfillment_Type"

        ]]
        df['batchid']=batchid

        df.to_sql('newchannel_wm_payment', con=engine, if_exists='append', index=False, index_label=False)
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
    'E:\pythonws\pythonws\pythonws\playwrighttest\data_proceed\csvs\wm_付款\payment明细 2.26-3.12.csv',attrjson)
    #



