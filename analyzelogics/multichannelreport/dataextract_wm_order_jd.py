
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
reporttype='dataextract_wm_order_jd'

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

        sql1 = f"""delete from  newchannel_wm_order_jd where batchid = '{batchid}' """
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
        df.rename(columns={"商家ID":"商家ID",
            "商家名称":"商家名称",
            "订单号":"订单号",
            "订单状态":"订单状态",
            "支付币种":"支付币种",
            "支付币种金额-售出金额":"支付币种金额_售出金额",
            "订单币种":"订单币种",
            "美元金额-售出金额":"美元金额_售出金额",
            "支付币种金额-运费金额":"支付币种金额_运费金额",
            "美元金额-运费金额":"美元金额_运费金额",
            "平台优惠劵平台承担金额":"平台优惠劵平台承担金额",
            "平台优惠券商家承担金额":"平台优惠券商家承担金额",
            "下单帐号":"下单帐号",
            "客户姓名":"客户姓名",
            "收货国家或地区":"收货国家或地区",
            "客户省份":"客户省份",
            "客户城市":"客户城市",
            "客户邮编":"客户邮编",
            "客户地址1":"客户地址1",
            "客户地址2":"客户地址2",
            "联系电话":"联系电话",
            "商家备注":"商家备注",
            "下单时间":"下单时间",
            "付款确认时间":"付款确认时间",
            "SKUID":"SKUID",
            "商品ID":"商品ID",
            "货号":"货号",
            "商品名称":"商品名称",
            "订购数量":"订购数量",
            "备货时间":"备货时间",
            "支付币种金额-EPT价":"支付币种金额_EPT价",
            "美元金额-EPT价":"美元金额_EPT价",
            "销售属性":"销售属性",
            "定制信息":"定制信息",
            "来源站点":"来源站点",
            "税种":"税种",
            "税点":"税点",
            "税值（支付币种的金额）":"税值_支付币种的金额",
            "税值（美元）":"税值_美元",
            "渠道站点":"渠道站点"
            },inplace=True)
        df['area'] = attrjson['area']
        df['country'] = attrjson['country']
        df['store'] = attrjson['store']
        df['week'] = attrjson['week']
        df['qijian'] = attrjson['qijian']

        df=df[['area','country','store','week','qijian',
               "商家ID",
                "商家名称",
                "订单号",
                "订单状态",
                "支付币种",
                "支付币种金额_售出金额",
                "订单币种",
                "美元金额_售出金额",
                "支付币种金额_运费金额",
                "美元金额_运费金额",
                "平台优惠劵平台承担金额",
                "平台优惠券商家承担金额",
                "下单帐号",
                "客户姓名",
                "收货国家或地区",
                "客户省份",
                "客户城市",
                "客户邮编",
                "客户地址1",
                "客户地址2",
                "联系电话",
                "商家备注",
                "下单时间",
                "付款确认时间",
                "SKUID",
                "商品ID",
                "货号",
                "商品名称",
                "订购数量",
                "备货时间",
                "支付币种金额_EPT价",
                "美元金额_EPT价",
                "销售属性",
                "定制信息",
                "来源站点",
                "税种",
                "税点",
                "税值_支付币种的金额",
                "税值_美元",
                "渠道站点"
        ]]
        df['batchid']=batchid

        df.to_sql('newchannel_wm_order_jd', con=engine, if_exists='append', index=False, index_label=False)
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
    'E:\pythonws\pythonws\pythonws\playwrighttest\data_proceed\csvs\wm_jd订单\orderDetail81749-1.xls',attrjson)
    #



