import pandas as pd
import streamlit as st
import time
import numpy as np

from analyzelogics.abakeyword.abakeywordanalyze import anaylyzefile
from analyzelogics.mailtool.mailcheckdup import check_dup, update_status, get_data, delete_mails, newaddress

st.set_page_config(page_title="email duplicate check", page_icon="📈")

# st.markdown("# email duplicate check")
st.sidebar.header("email duplicate check")
tab1, tab2 = st.tabs(["邮件查重", "邮件列表"])

key = st.secrets['seckey']
with st.sidebar:
    key_input = st.text_input(
        "请输入key",
        "",
        key="seckey",
    )
if key == key_input:
    with tab1:

            st.write(
                """email 查重"""
            )
            col1, col2 = st.columns(2)
            with col1:
                email_input=st.text_input(
                    "请输入邮箱",
                    "",
                    key="email_input",
                )
            status=0

            if email_input:
                # st.write(email_input)
                status,df=check_dup(email_input)
                # st.write(len(df))
                if status==0:
                    st.write('邮箱地址未使用过')
                    st.write(df)
                    if st.button('新增',key=1):
                        # print('fdfdfdfdf')
                        newaddress(email_input)
                        st.experimental_rerun()
                else:
                    # st.write('邮箱地址已发送过')
                    st.write(df)


            if status!=0:
                # st.write(df['status'][0])
                with col2:
                    status_option = st.selectbox(
                    '选择状态',
                    [df['status'][0]]+['未回复','已成交',  '交涉中'],
                    )

                if status_option!=df['status'][0]:
                # if st.button('更改状态提交'):
                    # print('fdfdfdfdf')
                    update_status(email_input,status_option)
                    st.experimental_rerun()
    with tab2:

        querymail = st.text_input('查询邮箱')
        if querymail:
            df=get_data(querymail)
        else:
            df=get_data()
        df['选择']=False
        edited_df=st.experimental_data_editor(df,key='maillist')
        deleteids=edited_df.loc[edited_df['选择']==True]['id'].tolist()
        print(deleteids)
        # st.write(deleteids)
        @st.cache_data
        def convert_df(df):
            # IMPORTANT: Cache the conversion to prevent computation on every rerun
            return df.to_csv().encode('utf-8')


        csv = convert_df(df)
        col1_maillist, col2_maillist,col3_maillist = st.columns(3)

        with col1_maillist:
            if st.button('删除',key=2):
                # print('fdfdfdfdf')
                delete_mails(deleteids)
                st.experimental_rerun()
        with col2_maillist:
            if st.button('刷新',key=3):
                # print('fdfdfdfdf')
                st.experimental_rerun()
        with col3_maillist:
            st.download_button(
                label="Download data as CSV",
                data=csv,
                file_name='email_list.csv',
                mime='text/csv',
            )

