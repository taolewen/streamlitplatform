import pandas as pd
import streamlit as st
import time
import numpy as np

from analyzelogics.abakeyword.abakeywordanalyze import anaylyzefile
from analyzelogics.mailtool.mailcheckdup import check_dup, update_status

st.set_page_config(page_title="email duplicate check", page_icon="📈")

st.markdown("# email duplicate check")
st.sidebar.header("email duplicate check")
st.write(
    """email 查重"""
)
col1, col2 = st.columns(2)
with col1:
    email_input=st.text_input(
        "请输入邮箱",
        "",
        key="placeholder",
    )
status=0

if email_input:
    # st.write(email_input)
    status,df=check_dup(email_input)
    if status==0:
        st.write('邮箱地址未使用过，已加入记录列表')
        st.write(df)

    else:
        st.write('邮箱地址已发送过')
        st.write(df)

if status!=0:
    with col2:
        status_option = st.selectbox(
        '选择状态',
        ('未回复','已成交',  '交涉中'))
    if st.button('更改状态提交'):
        update_status(email_input,status_option)
        st.experimental_rerun()

