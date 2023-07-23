import streamlit as st
import pandas as pd
import numpy as np

from analyzelogics.priceset.pricesetselect import get_platform, get_area, get_country, get_month, get_touchengmode, \
    get_erchengmode, get_feerate, cal_data
from dbs import  mysqlconn


with st.sidebar:
    platform=st.selectbox('平台',get_platform())
    area=st.selectbox('区域',get_area())
    country=st.selectbox('国家',get_country())
    month=st.selectbox('月份',get_month())
    touchengmode=st.selectbox('头程模式',get_touchengmode(area))
    erchengfulfilltype=st.selectbox('二程发货方式',['fba','fbm'])
    erchengmode=st.selectbox('二程类型',get_erchengmode(platform,area,erchengfulfilltype))
    isbusy=st.selectbox('运费方式',['高峰','非高峰'])
    isbusydict={'高峰':'busy','非高峰':'notbusy'}
    isbusy1=isbusydict[isbusy]

col1, col2= st.columns(2)
with col1:

    erpsku=st.text_input('erpsku')
with col2:
    usesku=st.text_input('使用sku')

col1, col2, col3 = st.columns(3)

with col1:
    invrentrate=float(st.text_input('仓租系数',value=get_feerate('仓租系数',platform,country)))
    commissionrate=float(st.text_input('佣金费率',value=get_feerate('佣金比率',platform,country)))

with col2:
    vatrate=float(st.text_input('VAT税率', value=get_feerate('VAT系数', platform, country)))
    otherrate=float(st.text_input('其他费用',value=get_feerate('其他费用比率',platform,country)))

with col3:
    waverate=float(st.text_input('波动系数', value=get_feerate('波动系数', platform, country)))
    if st.button('Rerun', key=1):
        # print('fdfdfdfdf')
        st.experimental_rerun()
tab1,tab2=st.tabs(['定价表','明细'])

with tab1:
    # print(st.session_state['isfilled'])
    if 'isfilled' not in st.session_state:
        st.session_state['df'] = cal_data(platform=platform,area=area,country=country,erpsku=erpsku,usesku=usesku,month=month,touchengmode=touchengmode,isbusy=isbusy,erchengfulfilltype=erchengfulfilltype,erchengmode=erchengmode,
                invrentrate=float(invrentrate),commissionrate=float(commissionrate),vatrate=float(vatrate),otherrate=float(otherrate),waverate=float(waverate))
        st.session_state['df']['广告投放']=None
        st.session_state['df']['预计定价']=None
        st.session_state['df']['数量']=1
        st.session_state['df']['前台毛利']=None
        st.session_state['isfilled'] = 1
        # st.session_state['df'] = st.data_editor(st.session_state['df'][['erp_sku','usesku','platform','area','country','exchange_rate','广告投放','预计定价','数量','前台毛利']],key='df1')
        st.session_state['df'] = st.data_editor(st.session_state['df'],key='df1')


        # st.session_state['df'] = pd.read_sql(f'''select mailaddress,status,name,1 test1,2 test2 from email_check''', con=mysqlconn)
        # st.session_state['df']['test3'] = st.session_state['df'].apply(lambda x: x.test1 + x.test2, axis=1)
        # st.session_state['isfilled'] = 1
        # st.session_state['df'] = st.data_editor(st.session_state['df'],key='df1')

    else:
        print(st.session_state['df'])
        st.session_state['df']['前台毛利']=st.session_state['df'].apply(lambda x:None if x.广告投放==None or x.预计定价==None else
        (float(x.预计定价)-(x.purchaseprice_o+x.transinv_fee_act+x.invfee+x.头程_原币+x.ercheng_act)-(x.rate_combine/100+float(x.广告投放)/100)*float(x.预计定价))*x.数量
                                                                    ,axis=1)
        st.session_state['df'] = st.data_editor(st.session_state['df'],key='df1')

with tab2:
    # print(st.session_state['isfilled'])
    if 'isfilled' not in st.session_state:
        st.session_state['df'] = cal_data(platform=platform,area=area,country=country,erpsku=erpsku,usesku=usesku,month=month,touchengmode=touchengmode,isbusy=isbusy,erchengfulfilltype=erchengfulfilltype,erchengmode=erchengmode,
                invrentrate=float(invrentrate),commissionrate=float(commissionrate),vatrate=float(vatrate),otherrate=float(otherrate),waverate=float(waverate))
        st.session_state['df']['广告投放']=None
        st.session_state['df']['预计定价']=None
        st.session_state['df']['数量']=1
        st.session_state['df']['前台毛利']=None
        st.session_state['isfilled'] = 1
        st.session_state['df'] = st.data_editor(st.session_state['df'],key='df2')


        # st.session_state['df'] = pd.read_sql(f'''select mailaddress,status,name,1 test1,2 test2 from email_check''', con=mysqlconn)
        # st.session_state['df']['test3'] = st.session_state['df'].apply(lambda x: x.test1 + x.test2, axis=1)
        # st.session_state['isfilled'] = 1
        # st.session_state['df'] = st.data_editor(st.session_state['df'],key='df1')

    else:
        print(st.session_state['df'])
        st.session_state['df']['前台毛利']=st.session_state['df'].apply(lambda x:None if x.广告投放==None or x.预计定价==None else
        (float(x.预计定价)-(x.purchaseprice_o+x.transinv_fee_act+x.invfee+x.头程_原币+x.ercheng_act)-(x.rate_combine/100+float(x.广告投放)/100)*float(x.预计定价))*x.数量
                                                                    ,axis=1)
        st.session_state['df'] = st.data_editor(st.session_state['df'],key='df2')

        # print(st.session_state['df'])
        # st.session_state['df']['test3'] = st.session_state['df'].apply(lambda x: x.test1 + x.test2, axis=1)
        # st.session_state['df'] = st.data_editor(st.session_state['df'],key='df1')

    print(st.session_state['df'])
    # st.session_state['df']['test3']=st.session_state['df'].apply(lambda x:x.test1+x.test2,axis=1)
    # def ddd():
    #     st.session_state['df']['test3'] = st.session_state['df'].apply(lambda x: x.test1 + x.test2, axis=1)
    #     return st.session_state['df']
    # print(st.session_state['df'])
    #
    # st.session_state['df']=st.data_editor(st.session_state['df'])
    # print(st.session_state['df'])
