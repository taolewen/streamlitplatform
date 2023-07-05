import pandas as pd

from dbs import mysqlconn


def check_dup(mailaddress,status=0,name='test'):
    df=pd.read_sql(f'''select mailaddress,status,name from email_check where mailaddress='{mailaddress}' ''',con=mysqlconn)
    print(df)
    print(len(df))
    if len(df)==0:
        status='未回复'
        sql=f''' insert into email_check (mailaddress,status,name) values ('{mailaddress}','{status}','{name}') '''
        cursor=mysqlconn.cursor()
        cursor.execute(sql)
        mysqlconn.commit()
        return 0,df
    else:
        return 1,df



def update_status(mailaddress,status):
    sql=f'''update email_check set status='{status}' where mailaddress='{mailaddress}' '''
    cursor = mysqlconn.cursor()
    cursor.execute(sql)
    mysqlconn.commit()

