# cd
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

import streamlit as st
host = st.secrets["mysql"]['host'],
user = st.secrets["mysql"]['user'],
password = st.secrets["mysql"]['password'],
db = st.secrets["mysql"]['database']
connstr = f"mysql+pymysql://{user[0]}:%s@{host[0]}:3306/{db}?charset=utf8" % quote_plus(f'{password[0]}')
engine = create_engine(connstr)
pd.set_option('display.max_columns', None)
reporttype='dataextract_ebay_trans'

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
        sql0 = f"""delete from  newchannel_ebay_trans where batchid = '{batchid}' """
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
        # df = pd.read_csv(path,skiprows=11)
        df = pd.read_csv(path,skiprows=11)

    except:
        try:
            f = open(path, 'rb')
            r = f.read()
            f_charinfo = chardet.detect(r)
            print(f_charinfo)
        except:
            print('chardet解析文件编码失败')
            traceback.print_exc()
            return 2, traceback.format_exc()
        # df = pd.read_csv(path ,skiprows=11, encoding=f_charinfo["encoding"])

        df = pd.read_csv(path , encoding=f_charinfo["encoding"],skiprows=11)
        print(df)

    try:
        batchid=getuid()
        df.rename(columns={
            "Transaction creation date": "交易创建日期",
            "Type": "类型",
            "Order number": "订单编号",
            "Legacy order ID": "旧订单编号",
            "Buyer username": "买家用户名",
            "Buyer name": "买家姓名",
            "Post to city": "收货人所在县/市",
            "Post to province/region/state": "运送至省/地区/州",
            "Post to postcode": "收货人邮政编码",
            "Post to country": "收货人所在国家/地区",
            "Net amount": "净额",
            "Payout currency": "发款货币",
            "Payout date": "发款日期",
            "Payout ID": "发款编号",
            "Payout method": "收款方式",
            "Payout status": "发款状态",
            "Reason for hold": "冻结原因",
            "Item ID": "物品编号",
            "Transaction ID": "交易编号",
            "Item title": "物品标题",
            "Custom label": "自定义标签",
            "Quantity": "数量",
            "Item subtotal": "物品小计",
            "Postage and packaging": "运费与处理费",
            "Seller collected tax": "卖家收取的税费",
            "eBay collected tax": "eBay 收取的税费",
            "Final value fee – fixed": "成交费 — 固定",
            "Final value fee – variable": "成交费 — 因品类而异",
            "Very high 'item not as described' fee": "“物品与描述不符”指数非常高的费用",
            "Below standard performance fee": "表现不合格的费用",
            "International fee": "跨国交易费用",
            "Gross transaction amount": "交易总金额",
            "Transaction currency": "交易货币",
            "Exchange rate": "汇率",
            "Reference ID": "参考编号",
            "Description": "描述"

        }, inplace=True)
        # df = pd.read_csv(path,skiprows=11)
        df.rename(columns={
            "收货人所在县/市": "收货人所在县市",
            "运送至省/地区/州": "运送至省地区州",
            "收货人所在国家/地区": "收货人所在国家地区",
            "eBay 收取的税费": "eBay收取的税费",
            "成交费 — 固定": "成交费_固定",
            "成交费 — 因品类而异": "成交费_因品类而异",
            "“物品与描述不符”指数非常高的费用": "物品与描述不符_指数非常高的费用"

        }, inplace=True)
        df['area'] = attrjson['area']
        df['country'] = attrjson['country']
        df['store'] = attrjson['store']
        df['week'] = attrjson['week']
        df['qijian']=attrjson['qijian']
        df['batchid']=batchid
        df['交易总金额'] = df['交易总金额'].astype(str)
        df['交易总金额']=df['交易总金额'].apply(lambda x:x.replace(',',''))
        df['净额'] = df['净额'].astype(str)
        df['净额']=df['净额'].apply(lambda x:x.replace(',',''))

        df = df[['area', 'country', 'store', 'week','qijian',
                 "交易创建日期",
                 "类型",
                 "订单编号",
                 "旧订单编号",
                 "买家用户名",
                 "买家姓名",
                 "收货人所在县市",
                 "运送至省地区州",
                 "收货人邮政编码",
                 "收货人所在国家地区",
                 "净额",
                 "发款货币",
                 "发款日期",
                 "发款编号",
                 "收款方式",
                 "发款状态",
                 "冻结原因",
                 "物品编号",
                 "交易编号",
                 "物品标题",
                 "自定义标签",
                 "数量",
                 "物品小计",
                 "运费与处理费",
                 "卖家收取的税费",
                 "eBay收取的税费",
                 "成交费_固定",
                 "成交费_因品类而异",
                 "物品与描述不符_指数非常高的费用",
                 "表现不合格的费用",
                 "跨国交易费用",
                 "交易总金额",
                 "交易货币",
                 "汇率",
                 "参考编号",
                 "描述",
                 "batchid"

                 ]]
        df['数量']=df.apply(lambda x:1 if x.类型=='退款' else x.数量,axis=1)
        df['物品小计']=df.apply(lambda x:x.交易总金额 if x.类型=='退款' else x.物品小计,axis=1)

        df.to_sql('newchannel_ebay_trans', con=engine, if_exists='append', index=False, index_label=False)
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

    attrjson = {
        'area': 'eu',
        'country': 'fr',
        'store': 'cd-3',
        'week': 20

    }
    dealsinglefile(
        'E:\pythonws\pythonws\pythonws\playwrighttest\data_proceed\csvs\mm_\manomano  (3.1-3.31).xlsx', attrjson)
    #



