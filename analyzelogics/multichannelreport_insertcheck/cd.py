
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


def check_report():
    sql_order='''select area,country,store,order_id,order_date,order_status,shipping_mode,
                    seller_reference,product_status,
                    quantity ,unit_price,shipping_fee,commission,
                    seller_income
                    from newchannel_cd_orderextract
                    where length(week)=6
                    '''
    df_order=pd.read_sql(sql_order,con=engine)





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

                '''
    df_settle=pd.read_sql(sql_settle,con=engine)













