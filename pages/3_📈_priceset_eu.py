import streamlit as st
import pandas as pd
import numpy as np
import streamlit_echarts
from pyecharts.charts import Bar
from pyecharts import options as opts
from pyecharts.globals import ThemeType
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
col1, col2,col3,col4= st.columns(4)
with col1:
    platform=st.selectbox('平台',get_platform())
    # area=st.selectbox('区域',get_area())
    area=st.selectbox('区域',['EU'])


with col2:
    country=st.selectbox('国家',get_country(area))
    month=st.selectbox('月份',get_month())


with col3:
    touchengmode=st.selectbox('头程模式',get_touchengmode(area))
    erchengfulfilltype=st.selectbox('二程发货方式',['fba','fbm'])


with col4:
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
with st.container():

    with col1:
        invrentrate=float(st.text_input('仓租系数',value=get_feerate('仓租系数',platform,country)))
        commissionrate=float(st.text_input('佣金费率',value=get_feerate('佣金比率',platform,country)))

    with col2:
        vatrate=float(st.text_input('VAT税率', value=get_feerate('VAT系数', platform, country)))
        otherrate=float(st.text_input('其他费用',value=get_feerate('其他费用比率',platform,country)))

    with col3:
        waverate=float(st.text_input('波动系数', value=get_feerate('波动系数', platform, country)))
        # if st.button('Rerun', key=1):
        #     # print('fdfdfdfdf')
        #     st.experimental_rerun()
tab1,tab2=st.tabs(['定价表','明细'])

with st.sidebar:
    ispaste = st.checkbox('粘贴模式')
    if ispaste:
        df_s1=pd.DataFrame(columns=['erp_sku','广告投放','预计定价','数量'])
        for  i in range(0,1000):
            df_s1.loc[i]=['',None,None,1]

        isbatchset = st.checkbox('统一设置参数')
        if isbatchset:
            ad = st.slider('广告投放', 0, 100, 8)
            preprice = st.slider('预计定价', 0, 500, 100)
            qty = st.slider('数量', 0, 100, 1)

            df_s1['广告投放']=ad
            df_s1['预计定价']=preprice
            df_s1['数量']=qty


        df_s1=st.data_editor(df_s1,hide_index=True)
    else:
        df_s=cal_data(platform=platform,area=area,country=country,erpsku=erpsku,usesku=usesku,month=month,touchengmode=touchengmode,isbusy=isbusy,erchengfulfilltype=erchengfulfilltype,erchengmode=erchengmode,
                    invrentrate=float(invrentrate),commissionrate=float(commissionrate),vatrate=float(vatrate),otherrate=float(otherrate),waverate=float(waverate))[['erp_sku']]
        df_s['广告投放'] = None
        df_s['预计定价'] = None
        df_s['数量'] = 1
        # df_s['前台毛利'] = None
        isbatchset = st.checkbox('统一设置参数')
        if isbatchset:
            ad = st.slider('广告投放', 0, 100, 8)
            preprice = st.slider('预计定价', 0, 500, 100)
            qty = st.slider('数量', 0, 100, 1)

            df_s['广告投放']=ad
            df_s['预计定价']=preprice
            df_s['数量']=qty
        df_s=st.data_editor(df_s,hide_index=True)

@st.cache_data
def convert_df(df):
    # IMPORTANT: Cache the conversion to prevent computation on every rerun
    return df.to_csv().encode('utf-8')

with tab1:
    if ispaste:
        df_m=cal_data(platform=platform,area=area,country=country,erpsku=erpsku,usesku=usesku,month=month,touchengmode=touchengmode,isbusy=isbusy,erchengfulfilltype=erchengfulfilltype,erchengmode=erchengmode,
                        invrentrate=float(invrentrate),commissionrate=float(commissionrate),vatrate=float(vatrate),otherrate=float(otherrate),waverate=float(waverate))
        df_m=pd.merge(df_s1,df_m,on=['erp_sku'],how='left')
        df_m['预计定价']=df_m['预计定价'].astype('float64')
        df_m['广告投放']=df_m['广告投放'].astype('float64')
        df_m['前台毛利']=df_m.apply(lambda x:None if x.广告投放==None or x.预计定价==None else
            round(((x.预计定价)-(x.purchaseprice_o+x.transinv_fee_act+x.invfee+x.头程_原币+x.ercheng_act)-(x.rate_combine/100+(x.广告投放)/100)*(x.预计定价))*x.数量,4)
                                                                        ,axis=1)
        df_m['前台毛利率']=df_m.apply(lambda x:None if x.广告投放==None or x.预计定价==None else
            round(x.前台毛利/(x.预计定价*x.数量),4)*100
                                                                        ,axis=1)
        df_m['0%利润价'] = df_m.apply(lambda x: None if x.广告投放 == None or x.预计定价 == None else
        (x.purchaseprice_o+x.transinv_fee_act+x.invfee+x.头程_原币+x.ercheng_act)/(1-(x.广告投放/100+x.rate_combine/100))
                                   , axis=1)
        df_m['5%利润价'] = df_m.apply(lambda x: None if x.广告投放 == None or x.预计定价 == None else
        (x.purchaseprice_o+x.transinv_fee_act+x.invfee+x.头程_原币+x.ercheng_act)/(1-(0.05+x.广告投放/100+x.rate_combine/100))
                                   , axis=1)
        df_m['10%利润价'] = df_m.apply(lambda x: None if x.广告投放 == None or x.预计定价 == None else
        (x.purchaseprice_o+x.transinv_fee_act+x.invfee+x.头程_原币+x.ercheng_act)/(1-(0.1+x.广告投放/100+x.rate_combine/100))
                                   , axis=1)
        df_m.rename(columns={'usesku':'使用sku','platform':'平台','area':'区域','country':'国家','height':'高','lenth':'长','width':'宽','uv':'体积','expansion_rate':'膨胀系数','exchange_rate':'汇率','purchaseprice':'采购价','purchaseprice_o':'采购价_原币','transinv_fee':'转仓费','invfee_rate':'仓租费率','invfee':'仓租',
                             'ercheng':'二程','rate_combine':'合并费率'},inplace=True)
        df_m=df_m[['erp_sku','使用sku','平台','区域','国家','广告投放','预计定价','数量','前台毛利','前台毛利率','0%利润价','5%利润价','10%利润价']]
        st.dataframe(df_m,
                     column_config={
        "前台毛利率": st.column_config.NumberColumn(
            "前台毛利率%",
            min_value=-500,
            max_value=500,
            # step=1,
            # format="%d %",
                ),
        "广告投放": st.column_config.NumberColumn(
            "广告投放%",
            min_value=-500,
            max_value=500,
            # step=1,
            # format="%d %",
                ),
        "前台毛利": st.column_config.ProgressColumn(
            "前台毛利",
            # help="The sales volume in USD",
            format="%f",
            min_value=-200,
            max_value=400,
        ),
            },

            hide_index=True)


    else:
        df_m=cal_data(platform=platform,area=area,country=country,erpsku=erpsku,usesku=usesku,month=month,touchengmode=touchengmode,isbusy=isbusy,erchengfulfilltype=erchengfulfilltype,erchengmode=erchengmode,
                        invrentrate=float(invrentrate),commissionrate=float(commissionrate),vatrate=float(vatrate),otherrate=float(otherrate),waverate=float(waverate))
        df_m=pd.merge(df_m,df_s,on=['erp_sku'],how='left')
        df_m['预计定价']=df_m['预计定价'].astype('float64')
        df_m['广告投放']=df_m['广告投放'].astype('float64')

        df_m['前台毛利']=df_m.apply(lambda x:None if x.广告投放==None or x.预计定价==None else
            round(((x.预计定价)-(x.purchaseprice_o+x.transinv_fee_act+x.invfee+x.头程_原币+x.ercheng_act)-(x.rate_combine/100+(x.广告投放)/100)*(x.预计定价))*x.数量,4)
                                                                        ,axis=1)
        df_m['前台毛利率']=df_m.apply(lambda x:None if x.广告投放==None or x.预计定价==None else
            round(x.前台毛利/(x.预计定价*x.数量),4)*100
                                                                        ,axis=1)
        df_m['0%利润价'] = df_m.apply(lambda x: None if x.广告投放 == None or x.预计定价 == None else
        (x.purchaseprice_o+x.transinv_fee_act+x.invfee+x.头程_原币+x.ercheng_act)/(1-(x.广告投放/100+x.rate_combine/100))
                                   , axis=1)
        df_m['5%利润价'] = df_m.apply(lambda x: None if x.广告投放 == None or x.预计定价 == None else
        (x.purchaseprice_o+x.transinv_fee_act+x.invfee+x.头程_原币+x.ercheng_act)/(1-(0.05+x.广告投放/100+x.rate_combine/100))
                                   , axis=1)
        df_m['10%利润价'] = df_m.apply(lambda x: None if x.广告投放 == None or x.预计定价 == None else
        (x.purchaseprice_o+x.transinv_fee_act+x.invfee+x.头程_原币+x.ercheng_act)/(1-(0.1+x.广告投放/100+x.rate_combine/100))
                                   , axis=1)
        df_m.rename(columns={'usesku':'使用sku','platform':'平台','area':'区域','country':'国家','height':'高','lenth':'长','width':'宽','uv':'体积','expansion_rate':'膨胀系数','exchange_rate':'汇率','purchaseprice':'采购价','purchaseprice_o':'采购价_原币','transinv_fee':'转仓费','invfee_rate':'仓租费率','invfee':'仓租',
                             'ercheng':'二程','rate_combine':'合并费率'},inplace=True)
        df_m=df_m[['erp_sku','使用sku','平台','区域','国家','广告投放','预计定价','数量','前台毛利','前台毛利率','0%利润价','5%利润价','10%利润价']]
        st.dataframe(df_m,
                     column_config={
        "前台毛利率": st.column_config.NumberColumn(
            "前台毛利率%",
            min_value=-500,
            max_value=500,
            # step=1,
            # format="%d %",
                ),
        "广告投放": st.column_config.NumberColumn(
            "广告投放%",
            min_value=-500,
            max_value=500,
            # step=1,
            # format="%d %",
                ),
        "前台毛利": st.column_config.ProgressColumn(
            "前台毛利",
            # help="The sales volume in USD",
            format="%f",
            min_value=-200,
            max_value=400,
        ),
            },

            hide_index=True)
    # if isbatchset:
    #     df_chart=df_m[['前台毛利','erp_sku']]
    #     df_chart.sort_values('前台毛利',ascending=False,inplace=True)
    #     df_chart=df_chart[:30]
    #     bar = Bar()
    #     bar.add_xaxis(df_chart['erp_sku'].tolist())
    #     bar.add_yaxis('前台毛利',df_chart['前台毛利'].tolist())
    #     streamlit_echarts.st_pyecharts(
    #         bar,
    #         theme=ThemeType.DARK
    #     )

with tab2:
    if ispaste:
        df_m=cal_data(platform=platform,area=area,country=country,erpsku=erpsku,usesku=usesku,month=month,touchengmode=touchengmode,isbusy=isbusy,erchengfulfilltype=erchengfulfilltype,erchengmode=erchengmode,
                        invrentrate=float(invrentrate),commissionrate=float(commissionrate),vatrate=float(vatrate),otherrate=float(otherrate),waverate=float(waverate))
        df_m=pd.merge(df_s1,df_m,on=['erp_sku'],how='left')
        df_m['预计定价']=df_m['预计定价'].astype('float64')
        df_m['广告投放']=df_m['广告投放'].astype('float64')
        df_m['前台毛利']=df_m.apply(lambda x:None if x.广告投放==None or x.预计定价==None else
            round(((x.预计定价)-(x.purchaseprice_o+x.transinv_fee_act+x.invfee+x.头程_原币+x.ercheng_act)-(x.rate_combine/100+(x.广告投放)/100)*(x.预计定价))*x.数量,4)
                                                                        ,axis=1)
        df_m['前台毛利率']=df_m.apply(lambda x:None if x.广告投放==None or x.预计定价==None else
            round(x.前台毛利/(x.预计定价*x.数量),4)*100
                                                                        ,axis=1)
        df_m['0%利润价'] = df_m.apply(lambda x: None if x.广告投放 == None or x.预计定价 == None else
        (x.purchaseprice_o+x.transinv_fee_act+x.invfee+x.头程_原币+x.ercheng_act)/(1-(x.广告投放/100+x.rate_combine/100))
                                   , axis=1)
        df_m['5%利润价'] = df_m.apply(lambda x: None if x.广告投放 == None or x.预计定价 == None else
        (x.purchaseprice_o+x.transinv_fee_act+x.invfee+x.头程_原币+x.ercheng_act)/(1-(0.05+x.广告投放/100+x.rate_combine/100))
                                   , axis=1)
        df_m['10%利润价'] = df_m.apply(lambda x: None if x.广告投放 == None or x.预计定价 == None else
        (x.purchaseprice_o+x.transinv_fee_act+x.invfee+x.头程_原币+x.ercheng_act)/(1-(0.1+x.广告投放/100+x.rate_combine/100))
                                   , axis=1)
        df_m.rename(columns={'usesku':'使用sku','platform':'平台','area':'区域','country':'国家','height':'高','lenth':'长','width':'宽','uv':'体积','expansion_rate':'膨胀系数','exchange_rate':'汇率','purchaseprice':'采购价','purchaseprice_o':'采购价_原币','transinv_fee':'转仓费','invfee_rate':'仓租费率','invfee':'仓租',
                             'ercheng':'二程','rate_combine':'合并费率'},inplace=True)
        # df_m=df_m[['erp_sku','使用sku','平台','区域','国家','广告投放','预计定价','数量','前台毛利','前台毛利率','0%利润价','5%利润价','10%利润价']]
        st.dataframe(df_m,
                     column_config={
        "前台毛利率": st.column_config.NumberColumn(
            "前台毛利率%",
            min_value=-500,
            max_value=500,
            # step=1,
            # format="%d %",
                ),
        "广告投放": st.column_config.NumberColumn(
            "广告投放%",
            min_value=-500,
            max_value=500,
            # step=1,
            # format="%d %",
                ),
        "前台毛利": st.column_config.ProgressColumn(
            "前台毛利",
            # help="The sales volume in USD",
            format="%f",
            min_value=-200,
            max_value=400,
        ),
            },

            hide_index=True)
    else:
        df_m=cal_data(platform=platform,area=area,country=country,erpsku=erpsku,usesku=usesku,month=month,touchengmode=touchengmode,isbusy=isbusy,erchengfulfilltype=erchengfulfilltype,erchengmode=erchengmode,
                        invrentrate=float(invrentrate),commissionrate=float(commissionrate),vatrate=float(vatrate),otherrate=float(otherrate),waverate=float(waverate))
        df_m=pd.merge(df_m,df_s,on=['erp_sku'],how='left')
        df_m['预计定价']=df_m['预计定价'].astype('float64')
        df_m['广告投放']=df_m['广告投放'].astype('float64')

        df_m['前台毛利']=df_m.apply(lambda x:None if x.广告投放==None or x.预计定价==None else
            round(((x.预计定价)-(x.purchaseprice_o+x.transinv_fee_act+x.invfee+x.头程_原币+x.ercheng_act)-(x.rate_combine/100+(x.广告投放)/100)*(x.预计定价))*x.数量,4)
                                                                        ,axis=1)
        df_m['前台毛利率']=df_m.apply(lambda x:None if x.广告投放==None or x.预计定价==None else
            round(x.前台毛利/(x.预计定价*x.数量),4)*100
                                                                        ,axis=1)
        df_m['0%利润价'] = df_m.apply(lambda x: None if x.广告投放 == None or x.预计定价 == None else
        (x.purchaseprice_o+x.transinv_fee_act+x.invfee+x.头程_原币+x.ercheng_act)/(1-(x.广告投放/100+x.rate_combine/100))
                                   , axis=1)
        df_m['5%利润价'] = df_m.apply(lambda x: None if x.广告投放 == None or x.预计定价 == None else
        (x.purchaseprice_o+x.transinv_fee_act+x.invfee+x.头程_原币+x.ercheng_act)/(1-(0.05+x.广告投放/100+x.rate_combine/100))
                                   , axis=1)
        df_m['10%利润价'] = df_m.apply(lambda x: None if x.广告投放 == None or x.预计定价 == None else
        (x.purchaseprice_o+x.transinv_fee_act+x.invfee+x.头程_原币+x.ercheng_act)/(1-(0.1+x.广告投放/100+x.rate_combine/100))
                                   , axis=1)
        df_m.rename(columns={'usesku':'使用sku','platform':'平台','area':'区域','country':'国家','height':'高','lenth':'长','width':'宽','uv':'体积','expansion_rate':'膨胀系数','exchange_rate':'汇率','purchaseprice':'采购价','purchaseprice_o':'采购价_原币','transinv_fee':'转仓费','invfee_rate':'仓租费率','invfee':'仓租',
                             'ercheng':'二程','rate_combine':'合并费率'},inplace=True)
        # df_m=df_m[['erp_sku','使用sku','平台','区域','国家','广告投放','预计定价','数量','前台毛利','前台毛利率','0%利润价','5%利润价','10%利润价']]
        st.dataframe(df_m,
                     column_config={
        "前台毛利率": st.column_config.NumberColumn(
            "前台毛利率%",
            min_value=-500,
            max_value=500,
            # step=1,
            # format="%d %",
                ),
        "广告投放": st.column_config.NumberColumn(
            "广告投放%",
            min_value=-500,
            max_value=500,
            # step=1,
            # format="%d %",
                ),
        "前台毛利": st.column_config.ProgressColumn(
            "前台毛利",
            # help="The sales volume in USD",
            format="%f",
            min_value=-200,
            max_value=400,
        ),
            },

            hide_index=True)
csv = convert_df(df_m)
st.download_button(
    label="Download data as CSV",
    data=csv,
    file_name='定价表.csv',
    mime='text/csv',
)