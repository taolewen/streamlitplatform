import pandas as pd
import pymysql

# from dbs import mysqlconn
import streamlit as st

def get_data(title=None):
    mysqlconn = pymysql.connect(host=st.secrets["mysql"]['host'],
                                user=st.secrets["mysql"]['user'],
                                password=st.secrets["mysql"]['password'],
                                db=st.secrets["mysql"]['database'])
    df=pd.read_sql(f'''select id,mailaddress,status,name from email_check  {'' if title==None else f'where mailaddress like "%{title}%" '}''',con=mysqlconn)
    return df
def check_dup(mailaddress,status=0,name='test'):
    mysqlconn = pymysql.connect(host=st.secrets["mysql"]['host'],
                                user=st.secrets["mysql"]['user'],
                                password=st.secrets["mysql"]['password'],
                                db=st.secrets["mysql"]['database'])
    df=pd.read_sql(f'''select mailaddress,status,name from email_check where mailaddress='{mailaddress.replace(' ','')}' ''',con=mysqlconn)
    print(f'''select mailaddress,status from email_check where mailaddress='{mailaddress.replace(' ','')}' ''')
    print(df)
    print(len(df))
    if len(df)==0:
        return 0,df
    else:
        return 1,df

def newaddress(mailaddress,name='test'):
    mysqlconn1 = pymysql.connect(host=st.secrets["mysql"]['host'],
                                 user=st.secrets["mysql"]['user'],
                                 password=st.secrets["mysql"]['password'],
                                 db=st.secrets["mysql"]['database'])
    status = '未回复'
    sql = f''' insert into email_check (mailaddress,status,name) values ('{mailaddress.replace(' ', '')}','{status}','{name}') '''
    cursor = mysqlconn1.cursor()
    cursor.execute(sql)
    mysqlconn1.commit()
    cursor.close()
    mysqlconn1.close()


def update_status(mailaddress,status):
    sql=f'''update email_check set status='{status}' where mailaddress='{mailaddress.replace(' ','')}' '''
    print(sql)
    mysqlconn2 = pymysql.connect(host=st.secrets["mysql"]['host'],
                                 user=st.secrets["mysql"]['user'],
                                 password=st.secrets["mysql"]['password'],
                                 db=st.secrets["mysql"]['database'])

    cursor = mysqlconn2.cursor()
    cursor.execute(sql)
    mysqlconn2.commit()
    cursor.close()
    mysqlconn2.close()
    print('update ok')


def delete_mails(mailids):

    mysqlconn2 = pymysql.connect(host=st.secrets["mysql"]['host'],
                                 user=st.secrets["mysql"]['user'],
                                 password=st.secrets["mysql"]['password'],
                                 db=st.secrets["mysql"]['database'])
    for id in mailids:
        sql=f'''delete from email_check where id={id}'''
        print(sql)

        cursor = mysqlconn2.cursor()
        cursor.execute(sql)
        mysqlconn2.commit()
        cursor.close()
        print('update ok')
    mysqlconn2.close()
