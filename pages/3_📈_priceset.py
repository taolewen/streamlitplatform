import streamlit as st
import pandas as pd
import numpy as np

from analyzelogics.priceset.pricesetselect import get_platform, get_area, get_country, get_month, get_touchengmode, \
    get_erchengmode, get_feerate, cal_data
from dbs import  mysqlconn


# with st.sidebar:
#     platform=st.selectbox('平台',get_platform())
#     area=st.selectbox('区域',get_area())
#     country=st.selectbox('国家',get_country())
#     month=st.selectbox('月份',get_month())
#     touchengmode=st.selectbox('头程模式',get_touchengmode(area))
#     erchengfulfilltype=st.selectbox('二程发货方式',['fba','fbm'])
#     erchengmode=st.selectbox('二程类型',get_erchengmode(platform,area,erchengfulfilltype))
#     isbusy=st.selectbox('运费方式',['高峰','非高峰'])
#     isbusydict={'高峰':'busy','非高峰':'notbusy'}
#     isbusy1=isbusydict[isbusy]
area=None
col1, col2,col3,col4= st.columns(4)
with col1:
    platform=st.selectbox('平台',get_platform())
    touchengmode=st.selectbox('头程模式',get_touchengmode(area))

with col2:
    area=st.selectbox('区域',get_area())
    erchengfulfilltype=st.selectbox('二程发货方式',['fba','fbm'])

with col3:

    country=st.selectbox('国家',get_country())
    erchengmode=st.selectbox('二程类型',get_erchengmode(platform,area,erchengfulfilltype))

with col4:
    month=st.selectbox('月份',get_month())
    isbusy=st.selectbox('运费方式',['高峰','非高峰'])
    isbusydict={'高峰':'busy','非高峰':'notbusy'}
    isbusy1=isbusydict[isbusy]

col1, col2= st.columns(2)
with col1:

    erpsku=st.text_input('erpsku')
with col2:
    usesku=st.text_input('使用sku')

col1, col2, col3 = st.columns(3)
with st.container():

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

with st.sidebar:
    ispaste = st.checkbox('粘贴模式')
    if ispaste:
        df_s1=pd.DataFrame(columns=['erp_sku','广告投放','预计定价','数量'])
        for  i in range(0,1000):
            df_s1.loc[i]=['',None,None,1]
        df_s1=st.data_editor(df_s1)
    else:
        df_s=cal_data(platform=platform,area=area,country=country,erpsku=erpsku,usesku=usesku,month=month,touchengmode=touchengmode,isbusy=isbusy,erchengfulfilltype=erchengfulfilltype,erchengmode=erchengmode,
                    invrentrate=float(invrentrate),commissionrate=float(commissionrate),vatrate=float(vatrate),otherrate=float(otherrate),waverate=float(waverate))[['erp_sku']]
        df_s['广告投放'] = None
        df_s['预计定价'] = None
        df_s['数量'] = 1
        # df_s['前台毛利'] = None
        df_s=st.data_editor(df_s)




if ispaste:
    df_m=cal_data(platform=platform,area=area,country=country,erpsku=erpsku,usesku=usesku,month=month,touchengmode=touchengmode,isbusy=isbusy,erchengfulfilltype=erchengfulfilltype,erchengmode=erchengmode,
                    invrentrate=float(invrentrate),commissionrate=float(commissionrate),vatrate=float(vatrate),otherrate=float(otherrate),waverate=float(waverate))
    df_m=pd.merge(df_s1,df_m,on=['erp_sku'],how='left')
    df_m['预计定价']=df_m['预计定价'].astype('float64')
    df_m['广告投放']=df_m['广告投放'].astype('float64')
    df_m['前台毛利']=df_m.apply(lambda x:None if x.广告投放==None or x.预计定价==None else
        ((x.预计定价)-(x.purchaseprice_o+x.transinv_fee_act+x.invfee+x.头程_原币+x.ercheng_act)-(x.rate_combine/100+(x.广告投放)/100)*(x.预计定价))*x.数量
                                                                    ,axis=1)
    st.dataframe(df_m)
else:
    df_m=cal_data(platform=platform,area=area,country=country,erpsku=erpsku,usesku=usesku,month=month,touchengmode=touchengmode,isbusy=isbusy,erchengfulfilltype=erchengfulfilltype,erchengmode=erchengmode,
                    invrentrate=float(invrentrate),commissionrate=float(commissionrate),vatrate=float(vatrate),otherrate=float(otherrate),waverate=float(waverate))
    df_m=pd.merge(df_m,df_s,on=['erp_sku'],how='left')
    df_m['预计定价']=df_m['预计定价'].astype('float64')
    df_m['广告投放']=df_m['广告投放'].astype('float64')

    df_m['前台毛利']=df_m.apply(lambda x:None if x.广告投放==None or x.预计定价==None else
        ((x.预计定价)-(x.purchaseprice_o+x.transinv_fee_act+x.invfee+x.头程_原币+x.ercheng_act)-(x.rate_combine/100+(x.广告投放)/100)*(x.预计定价))*x.数量
                                                                    ,axis=1)
    st.dataframe(df_m)