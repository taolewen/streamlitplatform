
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
# pd.set_option('display.max_columns', None)
# reporttype='dataextract_wm_ad'

def getuid():
    uid = str(uuid.uuid4())
    suid = ''.join(uid.split('-'))
    return suid
# def updatebatch(attrjson,batchid,path):
#     conn = pymysql.connect(host=st.secrets["mysql"]['host'],
#                                 user=st.secrets["mysql"]['user'],
#                                 password=st.secrets["mysql"]['password'],
#                                 db=st.secrets["mysql"]['database'])
#     cursor = conn.cursor()
#     sql = f"""insert newchannel_batchinfo (batchid,reporttype,path,area,country,week,store,qijian) values
#     ('{batchid}','{reporttype}','{path}','{attrjson['area'].upper()}','{(attrjson['country']).upper()}',
#     '{attrjson['week']}','{attrjson['store'].upper()}','{attrjson['qijian']}')"""
#     cursor.execute(sql)
#     conn.commit()
#     cursor.close()
#     conn.close()
# def selectbatch(attrjson):
#     sql = f"""select * from newchannel_batchinfo where reporttype='{reporttype}'
#     {f'''and area='{attrjson['area']}' ''' if attrjson['area'] else ''}
#     {f'''and country='{attrjson['country']}' ''' if attrjson['country'] else ''}
#     {f'''and store='{attrjson['store']}' ''' if attrjson['store'] else ''}
#     {f'''and week='{attrjson['week']}' ''' if attrjson['week'] else ''}
#     {f'''and qijian='{attrjson['qijian']}' ''' if attrjson['qijian'] else ''}
#     order by createdate desc"""
#     df=pd.read_sql(sql,con=connstr)
#     dl=df.to_dict('records')
#     strlist=[]
#     for d in dl:
#         try:
#             filename=d['path'].split('/')[-1]
#         except:
#             filename='notfound'
#         str=f"{d['createdate']}_{filename}_{d['area']}_{d['country']}_{d['store']}_{d['week']}_{d['batchid']}"
#         # print(str)
#         strlist.append(str)
#     def getfilename(x):
#         try:
#             return x.split('/')[-1]
#         except:
#             return 'none'
#     df['filename']=df.apply(lambda x:getfilename(x.path),axis=1)
#     df=df.drop('path', axis=1)
#     df['delete']=False
#     df=df[['delete','area','country','qijian','week','store','filename','createdate','reporttype','batchid']]
#     return df

def dealsinglefile(path,attrjson={}):
    try:
        batchid=getuid()

        col = ['Amazon Seller Account','Amazon Store Name', 'Amazon Ticket ID','SKU', 'Reimbursement ID', 'Approval Date', 'Amount', 'Currency','Type','Invoice Number','Marketplace']
        # col='A:K'
        df=pd.read_csv(path,usecols=col,encoding='cp1252')
        # df = pd.read_excel(path, sheet_name='账务明细列表', usecols='A:E', names=name)

        # df['area'] = attrjson['area']
        # df['country'] = attrjson['country']
        # df['store'] = attrjson['store']
        # df['week'] = attrjson['week']
        # df['qijian'] = attrjson['qijian']
        # df['Ad_Spend'].fillna('0',inplace=True)

        df.rename(columns={
            'Amazon Seller Account':'Amazon_Seller_Account',
            'Amazon Store Name':'Amazon_Store_Name',
            'Amazon Ticket ID': 'Amazon_Ticket_ID',
            'SKU': 'SKU',
            'Reimbursement ID': 'Reimbursement_ID',
            'Approval Date': 'Approval_Date',
            'Amount': 'Amount',
            'Currency': 'Currency',
            'Type': 'Type',
            'Invoice Number': 'Invoice_Number',
            'Marketplace': 'Marketplace',

        },inplace=True)

        df=df[['Amazon_Seller_Account',
                'Amazon_Store_Name',
                'Amazon_Ticket_ID',
                'SKU',
                'Reimbursement_ID',
                'Approval_Date',
                'Amount',
                'Currency',
                'Type',
                'Invoice_Number',
                'Marketplace'

        ]]
        df['batchid']=batchid

        df.to_sql('linggou_compensation_list', con=engine, if_exists='append', index=False, index_label=False)
        # updatebatch(attrjson,batchid,path)

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
    'E:\pythonws\pythonws\pythonws\playwrighttest\data_proceed\csvs\wm广告\suzhoumuqianxinxijisuyouxiangongsi.csv',attrjson)
    #



