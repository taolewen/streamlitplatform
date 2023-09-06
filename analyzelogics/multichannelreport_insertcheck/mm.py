
import datetime
import os
import traceback
import uuid
from datetime import timedelta
from urllib.parse import quote_plus

import numpy as np
import pymysql
from dateutil import parser
import pandas as pd

# 显示所有列
from sqlalchemy import create_engine


import streamlit as st
# host = st.secrets["mysql"]['host'],
# user = st.secrets["mysql"]['user'],
# password = st.secrets["mysql"]['password'],
# db = st.secrets["mysql"]['database']
# connstr = f"mysql+pymysql://{user[0]}:%s@{host[0]}:3306/{db}?charset=utf8" % quote_plus(f'{password[0]}')
connstr = "mysql+pymysql://developer:%s@124.71.174.53:3306/csbd?charset=utf8" % quote_plus('csbd@123')


engine = create_engine(connstr)


def check_report():
    sql_order='''
    select area,country,store,Date order_date,Order_Reference order_id,Product_reference sku,Quantity,Price_VAT_excluded,Shipping_costs_VAT_excluded,Price_VAT_included,Status
    from newchannel_mm_order
    
    '''
    df_order=pd.read_sql(sql_order,con=engine)
    df_order['Quantity'].fillna(0, inplace=True)
    df_order['Price_VAT_excluded'].fillna(0, inplace=True)
    df_order['Shipping_costs_VAT_excluded'].fillna(0, inplace=True)
    df_order['Price_VAT_included'].fillna(0, inplace=True)
    df_order['Quantity']=df_order['Quantity'].astype('float64')
    df_order['Price_VAT_excluded']=df_order['Price_VAT_excluded'].astype('float64')
    df_order['Shipping_costs_VAT_excluded']=df_order['Shipping_costs_VAT_excluded'].astype('float64')
    df_order['Price_VAT_included']=df_order['Price_VAT_included'].astype('float64')
    df_order=df_order.groupby(['area','country','store','order_date','order_id','sku','Status']).\
        agg({'Quantity':np.sum,'Price_VAT_excluded':np.sum,'Shipping_costs_VAT_excluded':np.sum,'Price_VAT_included':np.sum}).reset_index()
    df_order.to_csv('test_mm_order.csv')
    sql_settle='''
    select area,country,store,item_create_time,item_ref order_id,item_type,quantity,sku_seller sku,amount_vat_incl,amount_vat_excl,products_total,shipping_total,commission_vat_excl,tva_comission,
        commission_vat_incl,
        net_amount,
        coupons,
        currency,
        vat_amount,
        invoice_number invoice_num
    from newchannel_mm_payment
    where item_type!=' Item type' and item_type!='1'
    '''
    df_settle=pd.read_sql(sql_settle,con=engine)
    def linetype(x):
        if x=='Commande':
            return '订单'
        elif x=='Remboursement':
            return '退货'
        elif x=='Eco-contribution':
            return '其他'
        elif x=='Element de facture':
            return '广告费充值'
        elif x=='VAT adjustment':
            return '代缴VAT'
        elif x=='Regularisation':
            return 'Regularisation'
        else:
            return 'none'
    df_settle['linetype']=df_settle['item_type'].apply(lambda x:linetype(x))
    df_settle['quantity'].fillna(0, inplace=True)
    df_settle['amount_vat_incl'].fillna(0, inplace=True)
    df_settle['amount_vat_excl'].fillna(0, inplace=True)
    df_settle['products_total'].fillna(0, inplace=True)
    df_settle['shipping_total'].fillna(0, inplace=True)
    df_settle['commission_vat_excl'].fillna(0, inplace=True)
    df_settle['tva_comission'].fillna(0, inplace=True)
    df_settle['commission_vat_incl'].fillna(0, inplace=True)
    df_settle['net_amount'].fillna(0, inplace=True)
    df_settle['coupons'].fillna(0, inplace=True)
    df_settle['vat_amount'].fillna(0, inplace=True)
    df_settle['item_create_time'].fillna('none', inplace=True)
    df_settle['order_id'].fillna('none', inplace=True)
    df_settle['item_type'].fillna('none', inplace=True)
    df_settle['sku'].fillna('none', inplace=True)
    df_settle['currency'].fillna('none', inplace=True)
    df_settle['invoice_num'].fillna('none', inplace=True)

    df_settle=df_settle.groupby(['area','country','store','item_create_time','order_id','item_type','sku','currency','invoice_num','linetype']).\
        agg({'quantity':np.sum,'amount_vat_incl':np.sum,'amount_vat_excl':np.sum,'products_total':np.sum
                              ,'shipping_total':np.sum,'commission_vat_excl':np.sum,'tva_comission':np.sum
                              ,'commission_vat_incl':np.sum,'net_amount':np.sum,'coupons':np.sum,'vat_amount':np.sum}).reset_index()
    df_settle.to_csv('test_mm_settle.csv')
    df_settle_sub=df_settle[['area','country','store','order_id','item_create_time']]
    df_merge_os=pd.merge(df_order,df_settle_sub,on=['area','country','store','order_id'],how='left')
    df_merge_os.to_csv('test_mm_os.csv')
    df_order_sub=df_order[['area','country','store','order_id','order_date','Price_VAT_included']]
    df_merge_so=pd.merge(df_settle,df_order_sub,on=['area','country','store','order_id'],how='left')
    df_merge_so['diff_amount']=df_merge_so.apply(lambda x:x.amount_vat_incl-x.Price_VAT_included,axis=1)
    df_merge_so.to_csv('test_mm_so.csv')


if __name__ == '__main__':
    check_report()














