import pandas as pd
import streamlit as st
import time
import numpy as np

from analyzelogics.abakeyword.abakeywordanalyze import anaylyzefile

st.set_page_config(page_title="ABA keywords", page_icon="📈")

st.markdown("# ABA keywords")
st.sidebar.header("ABA keywords")
st.write(
    """ABAkeywords 根据单个单词出现次数汇总统计"""
)



# Streamlit widgets automatically run the script from top to bottom. Since
# this button is not connected to any other logic, it just causes a plain
# rerun.
uploaded_file = st.file_uploader("Choose a file")
if uploaded_file is not None:


    df_res=anaylyzefile(uploaded_file)
    # Can be used wherever a "file-like" object is accepted:
    st.write(df_res)


    @st.cache_data
    def convert_df_csv(df):
        # IMPORTANT: Cache the conversion to prevent computation on every rerun
        return df.to_csv().encode('utf-8')

    csv = convert_df_csv(df_res)
    st.download_button(
        label="下载文件为CSV",
        data=csv,
        file_name='关键词分析.csv',
        mime='text/csv',
    )

    cs=list(df_res.columns[1:])
    # print(cs)
    tabs=st.tabs(cs)
    i=0
    for tab in tabs:
        with tab:
            df_res_1=df_res[['客户搜索词',cs[i]]]
            df_res_1.sort_values(cs[i],inplace=True)
            print(df_res_1)
            st.bar_chart(df_res_1,x='客户搜索词',y=cs[i])
        i=i+1