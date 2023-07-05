import pandas as pd
import pymysql

from dbs import mysqlconn
import streamlit as st


def check_dup(mailaddress,status=0,name='test'):
    df=pd.read_sql(f'''select mailaddress,status,name from email_check where mailaddress='{mailaddress}' ''',con=mysqlconn)
    print(df)
    print(len(df))
    if len(df)==0:
        mysqlconn1 = pymysql.connect(host=st.secrets["mysql"]['host'],
                                    user=st.secrets["mysql"]['user'],
                                    password=st.secrets["mysql"]['password'],
                                    db=st.secrets["mysql"]['database'])
        status='未回复'
        sql=f''' insert into email_check (mailaddress,status,name) values ('{mailaddress}','{status}','{name}') '''
        cursor=mysqlconn1.cursor()
        cursor.execute(sql)
        mysqlconn.commit()
        cursor.close()
        mysqlconn1.close()
        return 0,df
    else:
        return 1,df



def update_status(mailaddress,status):
    sql=f'''update email_check set status='{status}' where mailaddress='{mailaddress}' '''
    print(sql)
    mysqlconn2 = pymysql.connect(host=st.secrets["mysql"]['host'],
                                 user=st.secrets["mysql"]['user'],
                                 password=st.secrets["mysql"]['password'],
                                 db=st.secrets["mysql"]['database'])

    cursor = mysqlconn2.cursor()
    cursor.execute(sql)
    mysqlconn.commit()
    cursor.close()
    mysqlconn2.close()
    print('update ok')