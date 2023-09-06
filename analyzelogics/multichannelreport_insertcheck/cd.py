
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
    sql_order='''select area,country,store,order_id,order_date,order_status,shipping_mode,
                    seller_reference,product_status,
                    quantity ,unit_price,shipping_fee,commission,
                    seller_income
                    from newchannel_cd_orderextract
                    where length(week)=6
                    '''
    df_order=pd.read_sql(sql_order,con=engine)
    df_order['order_id'].fillna('none',inplace=True)

    def linetype(x):
        if x.startswith('Frais prestation logistique'):
            return '平台运费'
        elif x.startswith('Avoir prestation logistique'):
            return '平台运费退回'
        elif x.startswith('Dépôt de garantie'):
            return '保证金-平台收取'
        elif x.startswith('Avoir de garantie'):
            return '保证金退回-平台返还'
        elif x.startswith('Facture service'):
            return '广告费-套餐外，'
        elif x.startswith('Facture abonnement'):
            return '广告费-套餐内；月租'
        elif x.startswith('Avoir abonnement'):
            return '其他费用'
        elif x.startswith('Avoir service'):
            return '其他费用'
        elif len(x)==15:
            return '订单'
        else:
            return 'none'

    df_order['order_date'].fillna('none',inplace=True)
    df_order['所属期间_order']=df_order.apply(lambda x:'none' if x.order_date=='none'
    else datetime.datetime.strptime(x.order_date,'%d/%m/%Y %H:%M:%S').strftime('%Y-%m-%d'),axis=1)
    df_order['order_status'].fillna('none',inplace=True)
    df_order['shipping_mode'].fillna('none',inplace=True)
    df_order['seller_reference'].fillna('none',inplace=True)
    df_order['product_status'].fillna('none',inplace=True)

    # df_order_skunna=df_order.loc[~pd.isnull(df_order['seller_reference'])]
    # df_order_skunum=df_order_skunna
    # df_order_skunum['skucount']=1
    # df_order_skunum=df_order_skunum.groupby(['area','country','store','order_id','order_date','order_status','shipping_mode','product_status']).\
    #     agg({'skucount':np.sum})
    # print(df_order_skunum)
    def getsku(x):
        slist=x.tolist()
        if 'none' in slist:
            slist.remove('none')
            return slist[0]
        else:
            return slist[0]
    df_order['sku']=df_order.groupby(['area','country','store','order_id','order_date','order_status','shipping_mode','product_status'])[['seller_reference']].transform(lambda x:str(getsku(x)))
    # df_order.to_csv('test.csv')
    # print(df_order)
    df_order['quantity']=df_order['quantity'].apply(lambda x:0 if x=='-' else x)
    df_order['unit_price']=df_order['unit_price'].apply(lambda x:0 if x=='-' else x)
    df_order['shipping_fee']=df_order['shipping_fee'].apply(lambda x:0 if x=='-' else x)
    df_order['commission']=df_order['commission'].apply(lambda x:0 if x=='-' else x)
    df_order['seller_income']=df_order['seller_income'].apply(lambda x:0 if x=='-' else x)
    df_order['quantity']=df_order['quantity'].astype('float64')
    df_order['shipping_fee']=df_order['shipping_fee'].astype('float64')
    df_order['commission']=df_order['commission'].astype('float64')
    df_order['seller_income']=df_order['seller_income'].astype('float64')

    df_order.to_csv('test_cd_order.csv')

    df_order=df_order.groupby(['area','country','store','order_id','order_date','所属期间_order','order_status','shipping_mode','sku','product_status']).\
        agg({'quantity':np.sum,'unit_price':np.sum,'shipping_fee':np.sum,'commission':np.sum,'seller_income':np.sum}).reset_index()


    sql_settle='''select area,country,store,n_facture_avoir,N_commande_service order_id,Date_opération_comptable settlement_date,
                 Vente_TTC_hors_frais_de_port,Frais_de_port_TTC,
                 Commission_Produit,
                Commission_Facilités_de_paiement,
                Commission_Frais_de_paiement_4_fois,
                Remboursement_TTC_hors_frais_de_port,
                Avoir_commission,
                Montant_de_TVA_sur_Vente_remboursement,
                Montant_de_TVA_sur_frais_de_port,
                Total_recu,
                Devise
                from 
                newchannel_cd_paymentdetail_settlement
                where length(week)=6
                and N_commande_service is not null
                '''
    df_settle=pd.read_sql(sql_settle,con=engine)
    df_settle['n_facture_avoir'].fillna('none',inplace=True)
    df_settle['order_id'].fillna('none',inplace=True)
    df_settle['settlement_date'].fillna('none',inplace=True)
    df_settle['所属期间_settle']=df_settle.apply(lambda x:'none' if x.settlement_date=='none'
    else datetime.datetime.strptime(x.settlement_date,'%d/%m/%Y').strftime('%Y-%m-%d'),axis=1)
    df_settle['Devise'].fillna('none',inplace=True)
    def getavoir(x):
        slist=x.tolist()
        if 'none' in slist and len(slist)!=1:
            slist.remove('none')
            return slist[0]
        else:
            return slist[0]
    df_settle['invoice_num']=df_settle.groupby(['area','country','store','order_id','settlement_date','Devise'])[['n_facture_avoir']].transform(lambda x:str(getavoir(x)))
    df_settle['Vente_TTC_hors_frais_de_port'].fillna(0,inplace=True)
    df_settle['Frais_de_port_TTC'].fillna(0,inplace=True)
    df_settle['Commission_Produit'].fillna(0,inplace=True)
    df_settle['Commission_Facilités_de_paiement'].fillna(0,inplace=True)
    df_settle['Commission_Frais_de_paiement_4_fois'].fillna(0,inplace=True)
    df_settle['Remboursement_TTC_hors_frais_de_port'].fillna(0,inplace=True)
    df_settle['Avoir_commission'].fillna(0,inplace=True)
    df_settle['Montant_de_TVA_sur_Vente_remboursement'].fillna(0,inplace=True)
    df_settle['Montant_de_TVA_sur_frais_de_port'].fillna(0,inplace=True)
    df_settle['Total_recu'].fillna(0,inplace=True)
    df_settle.to_csv('test_cd_settle.csv')
    # df_settle=df_settle.groupby(['area','country','store','order_id','invoice_num','settlement_date','Devise']).\
    #     agg({'Vente_TTC_hors_frais_de_port':np.sum,'Frais_de_port_TTC':np.sum,'Commission_Produit':np.sum,'Commission_Facilités_de_paiement':np.sum,'Commission_Frais_de_paiement_4_fois':np.sum,
    #             'Remboursement_TTC_hors_frais_de_port':np.sum,'Avoir_commission':np.sum,'Montant_de_TVA_sur_Vente_remboursement':np.sum,'Montant_de_TVA_sur_frais_de_port':np.sum,'Total_recu':np.sum
    #          }).reset_index()
    df_settle=df_settle.groupby(['area','country','store','order_id','Devise']).\
        agg(invoice_num=('invoice_num',','.join),settlement_date=('settlement_date',','.join),所属期间_settle=('所属期间_settle',','.join),
            Vente_TTC_hors_frais_de_port=('Vente_TTC_hors_frais_de_port',np.sum),Frais_de_port_TTC=('Frais_de_port_TTC',np.sum),
            Commission_Produit=('Commission_Produit',np.sum),Commission_Facilités_de_paiement=('Commission_Facilités_de_paiement',np.sum),
            Commission_Frais_de_paiement_4_fois=('Commission_Frais_de_paiement_4_fois',np.sum),Remboursement_TTC_hors_frais_de_port=('Remboursement_TTC_hors_frais_de_port',np.sum),
            Avoir_commission=('Avoir_commission',np.sum),Montant_de_TVA_sur_Vente_remboursement=('Montant_de_TVA_sur_Vente_remboursement',np.sum),
            Montant_de_TVA_sur_frais_de_port= ('Montant_de_TVA_sur_frais_de_port', np.sum),Total_recu=('Total_recu',np.sum)
            ).reset_index()


    df_settle.to_csv('test_cd_settle1.csv')

    df_merge_os=pd.merge(df_order,df_settle,on=['area','country','store','order_id'],how='outer')
    df_merge_os['linetype']=df_merge_os['order_id'].apply(lambda x:linetype(x))

    df_merge_os=df_merge_os[['area','country','store','order_id','order_date','所属期间_order','order_status','shipping_mode','sku','product_status','quantity','unit_price','shipping_fee','commission','seller_income','linetype',
                             'invoice_num','settlement_date','所属期间_settle','Devise',
                             'Vente_TTC_hors_frais_de_port','Frais_de_port_TTC','Commission_Produit','Commission_Facilités_de_paiement','Commission_Frais_de_paiement_4_fois',
                             'Remboursement_TTC_hors_frais_de_port','Avoir_commission','Montant_de_TVA_sur_Vente_remboursement','Montant_de_TVA_sur_frais_de_port','Total_recu']]
    df_merge_os['diff_totalrecu_sincome_ttc_4fois']=df_merge_os.apply(lambda x:x.Total_recu-x.Frais_de_port_TTC-x.Commission_Frais_de_paiement_4_fois-x.seller_income,axis=1)
    df_merge_os['diff_totalrecu_sincome_ttc_4fois']=df_merge_os['diff_totalrecu_sincome_ttc_4fois'].apply(lambda x:None if np.isnan(x) else round(x,2))
    df_merge_os.to_csv('test_cd_os.csv')
    sql_returnmoney='''select area,country,store,n_facture_avoir,N_commande_service order_id,Date_opération_comptable returnmoney_date,
                 Vente_TTC_hors_frais_de_port,Frais_de_port_TTC,
                 Commission_Produit,
                Commission_Facilités_de_paiement,
                Commission_Frais_de_paiement_4_fois,
                Remboursement_TTC_hors_frais_de_port,
                Avoir_commission,
                Montant_de_TVA_sur_Vente_remboursement,
                Montant_de_TVA_sur_frais_de_port,
                Total_recu,
                Devise,
                batchid
                from 
                newchannel_cd_paymentdetail_returnmoney
                where length(week)=6
                and N_commande_service is not null

                '''
    df_returnmoney=pd.read_sql(sql_returnmoney,con=engine)
    df_returnmoney['n_facture_avoir'].fillna('none',inplace=True)
    df_returnmoney['order_id'].fillna('none',inplace=True)
    df_returnmoney['returnmoney_date'].fillna('none',inplace=True)
    df_returnmoney['所属期间_returnmoney']=df_returnmoney.apply(lambda x:'none' if x.returnmoney_date=='none'
    else datetime.datetime.strptime(x.returnmoney_date,'%d/%m/%Y').strftime('%Y-%m-%d'),axis=1)
    df_returnmoney['Devise'].fillna('none',inplace=True)
    def getavoir(x):
        slist=x.tolist()
        if 'none' in slist and len(slist)!=1:
            slist.remove('none')
            return slist[0]
        else:
            return slist[0]
    df_returnmoney['invoice_num']=df_returnmoney.groupby(['area','country','store','order_id','returnmoney_date','Devise'])[['n_facture_avoir']].transform(lambda x:str(getavoir(x)))
    df_returnmoney['Vente_TTC_hors_frais_de_port'].fillna(0,inplace=True)
    df_returnmoney['Frais_de_port_TTC'].fillna(0,inplace=True)
    df_returnmoney['Commission_Produit'].fillna(0,inplace=True)
    df_returnmoney['Commission_Facilités_de_paiement'].fillna(0,inplace=True)
    df_returnmoney['Commission_Frais_de_paiement_4_fois'].fillna(0,inplace=True)
    df_returnmoney['Remboursement_TTC_hors_frais_de_port'].fillna(0,inplace=True)
    df_returnmoney['Avoir_commission'].fillna(0,inplace=True)
    df_returnmoney['Montant_de_TVA_sur_Vente_remboursement'].fillna(0,inplace=True)
    df_returnmoney['Montant_de_TVA_sur_frais_de_port'].fillna(0,inplace=True)
    df_returnmoney['Total_recu'].fillna(0,inplace=True)
    df_returnmoney.to_csv('test_cd_returnmoney.csv')

    # df_returnmoney=df_returnmoney.groupby(['area','country','store','order_id','invoice_num','returnmoney_date','Devise','batchid']).\
    #     agg({'Vente_TTC_hors_frais_de_port':np.sum,'Frais_de_port_TTC':np.sum,'Commission_Produit':np.sum,'Commission_Facilités_de_paiement':np.sum,'Commission_Frais_de_paiement_4_fois':np.sum,
    #             'Remboursement_TTC_hors_frais_de_port':np.sum,'Avoir_commission':np.sum,'Montant_de_TVA_sur_Vente_remboursement':np.sum,'Montant_de_TVA_sur_frais_de_port':np.sum,'Total_recu':np.sum
    #          }).reset_index()
    df_returnmoney=df_returnmoney.groupby(['area','country','store','order_id','Devise']).\
        agg(invoice_num=('invoice_num',','.join),returnmoney_date=('returnmoney_date',','.join),所属期间_returnmoney=('所属期间_returnmoney',','.join),
            Vente_TTC_hors_frais_de_port=('Vente_TTC_hors_frais_de_port',np.sum),Frais_de_port_TTC=('Frais_de_port_TTC',np.sum),
            Commission_Produit=('Commission_Produit',np.sum),Commission_Facilités_de_paiement=('Commission_Facilités_de_paiement',np.sum),
            Commission_Frais_de_paiement_4_fois=('Commission_Frais_de_paiement_4_fois',np.sum),Remboursement_TTC_hors_frais_de_port=('Remboursement_TTC_hors_frais_de_port',np.sum),
            Avoir_commission=('Avoir_commission',np.sum),Montant_de_TVA_sur_Vente_remboursement=('Montant_de_TVA_sur_Vente_remboursement',np.sum),
            Montant_de_TVA_sur_frais_de_port= ('Montant_de_TVA_sur_frais_de_port', np.sum),Total_recu=('Total_recu',np.sum)
            ).reset_index()


    df_returnmoney.to_csv('test_cd_returnmoney1.csv')

    df_returnmoney.rename(columns={
        'Devise': 'returnmoney_Devise',
        'invoice_num':'returnmoney_invoice_num',
        'Vente_TTC_hors_frais_de_port': 'returnmoney_Vente_TTC_hors_frais_de_port',
        'Frais_de_port_TTC': 'returnmoney_Frais_de_port_TTC',
        'Commission_Produit': 'returnmoney_Commission_Produit',
        'Commission_Facilités_de_paiement': 'returnmoney_Commission_Facilités_de_paiement',
        'Commission_Frais_de_paiement_4_fois': 'returnmoney_Commission_Frais_de_paiement_4_fois',
        'Remboursement_TTC_hors_frais_de_port': 'returnmoney_Remboursement_TTC_hors_frais_de_port',
        'Avoir_commission': 'returnmoney_Avoir_commission',
        'Montant_de_TVA_sur_Vente_remboursement': 'returnmoney_Montant_de_TVA_sur_Vente_remboursement',
        'Montant_de_TVA_sur_frais_de_port': 'returnmoney_Montant_de_TVA_sur_frais_de_port',
        'Total_recu': 'returnmoney_Total_recu'
    },inplace=True)
    df_merge_sr=pd.merge(df_settle,df_returnmoney,on=['area','country','store','order_id'],how='outer')
    df_merge_sr['diff_totalrecu']=df_merge_sr.apply(lambda x:x.Total_recu-x.returnmoney_Total_recu,axis=1)
    df_merge_sr['diff_totalrecu']=df_merge_sr['diff_totalrecu'].apply(lambda x:None if np.isnan(x) else round(x,2))

    df_merge_sr.to_csv('test_cd_sr.csv')







if __name__ == '__main__':
    check_report()










