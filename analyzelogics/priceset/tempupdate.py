import pandas as pd
import pymysql

# from dbs import mysqlconn
import streamlit as st
from numpy import float64

def get_one_temp(username,tempname):
    mysqlconn = pymysql.connect(host=st.secrets["mysql"]['host'],
                                user=st.secrets["mysql"]['user'],
                                password=st.secrets["mysql"]['password'],
                                db=st.secrets["mysql"]['database'])
    df=pd.read_sql(f'''select platform,area,country,month,touchengmode,erchengfulfilltype,erchengmode,isbusy from priceset_temp where username='{username}' and tempname='{tempname}' ''',con=mysqlconn)
    def transbusy(x):
        isbusydict={'busy':'高峰','notbusy':'非高峰'}
        isbusy1=isbusydict[x]
        return isbusy1
    df['isbusy1']=df['isbusy'].apply(lambda x:transbusy(x))
    dictlist=df.to_dict(orient='records')
    return dictlist[0]
def get_user():
    mysqlconn = pymysql.connect(host=st.secrets["mysql"]['host'],
                                user=st.secrets["mysql"]['user'],
                                password=st.secrets["mysql"]['password'],
                                db=st.secrets["mysql"]['database'])
    df=pd.read_sql(f'''select username from priceset_temp group by username''',con=mysqlconn)
    return ['']+df['username'].to_list()
def get_temp(username):
    mysqlconn = pymysql.connect(host=st.secrets["mysql"]['host'],
                                user=st.secrets["mysql"]['user'],
                                password=st.secrets["mysql"]['password'],
                                db=st.secrets["mysql"]['database'])
    df=pd.read_sql(f'''select tempname from priceset_temp where username='{username}' group by tempname''',con=mysqlconn)
    return ['']+df['tempname'].to_list()
def savetemp2db(platform,area,country,month,touchengmode,erchengfulfilltype,erchengmode,isbusy1,username,tempname):
    mysqlconn = pymysql.connect(host=st.secrets["mysql"]['host'],
                                user=st.secrets["mysql"]['user'],
                                password=st.secrets["mysql"]['password'],
                                db=st.secrets["mysql"]['database'])
    sql=f'''insert into priceset_temp (platform,area,country,month,touchengmode,erchengfulfilltype,erchengmode,isbusy ,username,tempname)
            values ('{platform}','{area}','{country}','{month}','{touchengmode}','{erchengfulfilltype}','{erchengmode}','{isbusy1}','{username}','{tempname}')
            
            '''
    cursor=mysqlconn.cursor()
    cursor.execute(sql)
    mysqlconn.commit()
    cursor.close()
    mysqlconn.close()


def updatetempindb(platform,area,country,month,touchengmode,erchengfulfilltype,erchengmode,isbusy1,username,tempname):
    mysqlconn = pymysql.connect(host=st.secrets["mysql"]['host'],
                                user=st.secrets["mysql"]['user'],
                                password=st.secrets["mysql"]['password'],
                                db=st.secrets["mysql"]['database'])

    sql=f'''update priceset_temp set platform='{platform}',area='{area}',country='{country}',month='{month}',touchengmode='{touchengmode}',
    erchengfulfilltype='{erchengfulfilltype}',erchengmode='{erchengmode}',isbusy='{isbusy1}'  where username='{username}' and tempname='{tempname}' '''
    cursor = mysqlconn.cursor()
    cursor.execute(sql)
    mysqlconn.commit()
    cursor.close()
    mysqlconn.close()
