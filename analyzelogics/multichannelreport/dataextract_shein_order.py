
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
reporttype='dataextract_shein_order'

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
        sql0 = f"""delete from  newchannel_shein_order where batchid = '{batchid}' """
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
        # name=['gsp_orderid','sellersku','pro_price','username','postcode','country','province','city','user_address1','user_address2','phonenum']
        if len(str(attrjson['week']))==8:
            if attrjson['country']=='US':
                name=['订单类型','订单号','换货订单','订单状态','发货模式','是否催发','是否丢件','是否滞留','问题订单','商品名称',
                      '货号','规格','卖家SKU','SHEIN_SKU','SKC','商品ID','商品状态','库存标识','换货标识','换货原因','被换商品ID','是否锁定','分单时间','要求签收时间',
                      '运单号','卖家包裹','卖家币种','商品价格','优惠券金额','店铺活动优惠金额','佣金','预计收入','消费税']
                df = pd.read_excel(path, usecols='A:AG', names=name)
            else:
                name=['订单类型','订单号','站点','换货订单','订单状态','发货模式','是否催发','是否丢件','是否滞留','问题订单','商品名称',
                      '货号','规格','卖家SKU','SHEIN_SKU','SKC','商品ID','商品状态','库存标识','换货标识','换货原因','被换商品ID','是否锁定','分单时间',
                      '运单号','卖家包裹','卖家币种','商品价格','优惠券金额','店铺活动优惠金额','佣金','预计收入','消费税']
                df = pd.read_excel(path, usecols='A:AG', names=name)
                df['要求签收时间']=None
        else:
            if attrjson['country']=='US':
                name=['订单类型','订单号','换货订单','订单状态','发货模式','是否催发','是否丢件','是否滞留','问题订单','商品名称',
                      '货号','规格','卖家SKU','SHEIN_SKU','SKC','商品ID','商品状态','库存标识','换货标识','换货原因','被换商品ID','是否锁定','分单时间','要求签收时间',
                      '运单号','卖家包裹','卖家币种','商品价格','优惠券金额','店铺活动优惠金额','佣金','预计收入','消费税']
                df = pd.read_excel(path, usecols='A:AG', names=name)
            elif attrjson['country']=='DE':
                name=['订单类型','订单号','换货订单','订单状态','发货模式','是否催发','是否丢件','是否滞留','问题订单','商品名称',
                      '货号','规格','卖家SKU','SHEIN_SKU','SKC','商品ID','商品状态','库存标识','换货标识','换货原因','被换商品ID','是否锁定','分单时间',
                      '运单号','卖家包裹','卖家币种','商品价格','优惠券金额','店铺活动优惠金额','佣金','预计收入','欧洲vat税','预计商品收入_欧洲vat税后']
                df = pd.read_excel(path, usecols='A:AG', names=name)
                df['要求签收时间']=None
                df['消费税']=0

            else:
                name=['订单类型','订单号','站点','换货订单','订单状态','发货模式','是否催发','是否丢件','是否滞留','问题订单','商品名称',
                      '货号','规格','卖家SKU','SHEIN_SKU','SKC','商品ID','商品状态','库存标识','换货标识','换货原因','被换商品ID','是否锁定','分单时间',
                      '运单号','卖家包裹','卖家币种','商品价格','优惠券金额','店铺活动优惠金额','佣金','预计收入','消费税']
                df = pd.read_excel(path, usecols='A:AD', names=name)
                df['要求签收时间']=None

        # df=pd.read_excel(path,sheet_name='soges MF-退款')
        # df.to_csv('fdfd0.csv')
        # print(df)
        df['area'] = attrjson['area']
        df['country'] = attrjson['country']
        df['store'] = attrjson['store']
        df['week'] = attrjson['week']
        df['qijian']=attrjson['qijian']
        df['batchid']=batchid
        # df.to_csv('fdfd.csv')

        # df=df[['area','country','store','week','qijian',
        #        'gsp_orderid','sellersku','pro_price','username','postcode','country','province','city','user_address1','user_address2','phonenum',
        #        "batchid"
        # ]]
        df=df[['area','country','store','week','qijian',
               '订单类型', '订单号', '换货订单', '订单状态', '发货模式', '是否催发', '是否丢件', '是否滞留', '问题订单', '商品名称',
               '货号', '规格', '卖家SKU', 'SHEIN_SKU', 'SKC', '商品ID', '商品状态', '库存标识', '换货标识', '换货原因', '被换商品ID', '是否锁定',
               '分单时间', '要求签收时间',
               '运单号', '卖家包裹', '卖家币种', '商品价格','优惠券金额','店铺活动优惠金额','佣金','预计收入','消费税',
               "batchid"
        ]]

        df.to_sql('newchannel_shein_order', con=engine, if_exists='append', index=False, index_label=False)
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



