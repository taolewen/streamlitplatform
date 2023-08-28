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
reporttype='dataextract_ebay_orders'

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
        sql0 = f"""delete from  newchannel_ebay_orders where batchid = '{batchid}' """
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

        df = pd.read_csv(path,skiprows=1,skipfooter=3)
        cols=df.columns.tolist()
        cols=list(map(lambda x: x.replace('/','').replace(' ','').replace('&','_and_'),cols))
        # print((cols))
        df.columns=cols
        # df.to_csv('ebayordrestest.csv')
        df['area'] = attrjson['area']
        df['country'] = attrjson['country']
        df['store'] = attrjson['store']
        df['week'] = attrjson['week']
        df['qijian']=attrjson['qijian']
        df['batchid']=batchid

        df=df[[
            'area', 'country', 'store', 'week', 'qijian',
            "销售记录编号",
            "订单编号",
            # "买家用户名",
            # "买家姓名",
            # "买家电子邮件",
            # "买家备注",
            # "买家地址1",
            # "买家地址2",
            # "买家所在县市",
            # "买家所在州省",
            # "买家邮政编码",
            # "买家所在国家地区",
            # "买家纳税识别名称",
            # "买家纳税识别值",
            # "收货人姓名",
            # "收货人电话",
            # "收货地址1",
            # "收货地址2",
            # "收货人所在县市",
            # "收货人所在州省",
            # "收货人邮政编码",
            # "收货人所在国家地区",
            "物品编号",
            "物品标题",
            "自定义标签",
            "通过促销刊登售出",
            "数量",
            "成交价",
            "运费与处理费",
            "物品所在地",
            "物品所在地邮政编码",
            "物品所在国家地区",
            "eBay收缴税率",
            "eBay收缴税费类型",
            "eBay参考名",
            "eBay参考值",
            "税收状态",
            "卖家收取的税费",
            "eBay收取的税费",
            "电子垃圾回收费用",
            "床垫回收费用",
            "电池回收费用",
            "大型家电处置税",
            "轮胎回收费用",
            "其他费用",
            "总价",
            "总额包含eBay收取的税费和费用",
            "支付方式",
            "出售日期",
            "付款日期",
            "发货截止日期",
            "预计最早送达日期",
            "预计最晚送达日期",
            "发货日期",
            # "留下的信用评价",
            # "收到的信用评价",
            # "我的物品备注",
            "PayPal交易编号",
            "运送服务",
            "追踪号码",
            "交易编号",
            "款式详情",
            # "全球运送方案",
            # "全球运送参考编号",
            # "Click_and_Collect",
            # "Click_and_Collect参考编号",
            # "eBayPlus",
            # "正品验证计划",
            # "正品验证状态",
            # "正品验证结果理由",
            # "eBay履单服务方案",
            # "课税县市",
            # "课税州省",
            # "课税邮政编码",
            # "课税国家地区"
            'batchid',
        ]]

        df.to_sql('newchannel_ebay_orders', con=engine, if_exists='append', index=False, index_label=False)
        updatebatch(attrjson,batchid,path)

        return 1,''
    except:
        return 2,traceback.format_exc()




if __name__ == '__main__':
    dealsinglefile('C:\\Users\维斯特卢1-26\Desktop\eBay-OrdersReport-DE(6).csv','')


