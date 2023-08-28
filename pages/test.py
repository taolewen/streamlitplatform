"""
# My first app
Here's our first attempt at using data to create a table:
"""
#input 客服、客户
import pymysql
import streamlit as st
import pandas as pd

import streamlit.components.v1 as components



import streamlit as st


# Let's say the user's web browser is at http://localhost:8501/?show_map=True&selected=asia&selected=america.
# Then, you can get the query parameters using the following:
# st.experimental_get_query_params()
# {"show_map": ["True"], "selected": ["asia", "america"]}


# http://localhost:8501/?id=1

# {"id": ["1"]}
mysqlconn = pymysql.connect(host=st.secrets["mysql"]['host'],
                                user=st.secrets["mysql"]['user'],
                                password=st.secrets["mysql"]['password'],
                                db=st.secrets["mysql"]['database'])

st.experimental_get_query_params()
if st.experimental_get_query_params():
    sql=f'''select email.mail_from,email.mail_to,
    email.subject,email.recv_date,email.html,email.plain,att.attach_url 
    from email_gmail_copy1 email
    left join email_gmail_attachments_copy1 att on email.msgid=att.msgid 
    and email.ownaddress=att.ownaddress
    where id={st.experimental_get_query_params()['id'][0]}'''
    df=pd.read_sql(sql,con=mysqlconn)
    mailfrom=df['mail_from'].values[0]
    mailto=df['mail_to'].values[0]
    subject=df['subject'].values[0]
    recv_date=df['recv_date'].values[0]
    html=df['html'].values[0]
    plain=df['plain'].values[0]
    attach_url=df['attach_url'].values[0]

    st.write('发件人: '+mailfrom)
    st.write('收件人: '+mailto)
    st.write('日期: '+recv_date)
    st.write('主题: '+subject)
    st.write('附件: '+str(attach_url))

    # bootstrap 4 collapse example
    if html:
        components.html(html,height=600,width=800,scrolling=True)
    else:
        components.html(plain,height=600,width=800,scrolling=True)
