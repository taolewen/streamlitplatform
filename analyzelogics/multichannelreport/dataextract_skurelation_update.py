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

connstr = "mysql+pymysql://developer:%s@124.71.174.53:3306/csbd?charset=utf8" % quote_plus('csbd@123')
engine = create_engine(connstr)
pd.set_option('display.max_columns', None)




def dealsinglefile(path):
    try:



        # df = pd.read_excel(path,sheet_name='soges MM-广告')
        df = pd.read_excel(path)

        df = df[['country','store','seller_SKU','ERP_SKU'
                 ]]

        conn = pymysql.connect(host='124.71.174.53',
                               user='developer',
                               password='csbd@123',
                               database='csbd')
        cursor = conn.cursor()
        sql = f"""truncate table new_channel_correspondence """
        cursor.execute(sql)
        conn.commit()
        cursor.close()
        conn.close()

        df.to_sql('new_channel_correspondence', con=engine, if_exists='append', index=False, index_label=False)

        return 1,''
    except:
        return 2,traceback.format_exc()
