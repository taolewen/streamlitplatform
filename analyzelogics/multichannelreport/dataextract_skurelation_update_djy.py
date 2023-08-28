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




def dealsinglefile(path):
    try:

        # df = pd.read_excel(path,sheet_name='soges MM-广告')
        df = pd.read_excel(path)

        df = df[['国家','渠道','店铺SKU','大建云SKU','含保险采购价'
                 ]]

        conn = pymysql.connect(host=st.secrets["mysql"]['host'],
                               user=st.secrets["mysql"]['user'],
                               password=st.secrets["mysql"]['password'],
                               db=st.secrets["mysql"]['database'])
        cursor = conn.cursor()
        sql = f"""truncate table new_channel_correspondence_djy """
        cursor.execute(sql)
        conn.commit()
        cursor.close()
        conn.close()

        df.to_sql('new_channel_correspondence_djy', con=engine, if_exists='append', index=False, index_label=False)

        return 1,''
    except:
        return 2,traceback.format_exc()
