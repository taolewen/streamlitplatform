import datetime

import streamlit as st
import pandas as pd
import numpy as np
import streamlit_echarts
from pyecharts.charts import Bar
from pyecharts import options as opts
from pyecharts.globals import ThemeType
from analyzelogics.priceset.pricesetselect import get_platform, get_area, get_country, get_month, get_touchengmode, \
    get_erchengmode, get_feerate, cal_data, get_testitems, update_testitems
from analyzelogics.priceset.tempupdate import savetemp2db, get_user, get_temp, updatetempindb, get_one_temp
from dbs import  mysqlconn
from streamlit_modal import Modal


if "username" not in st.session_state:
    st.session_state["username"] = None
if "tempname" not in st.session_state:
    st.session_state["tempname"] = None
if "platform" not in st.session_state:
    st.session_state["platform"] = None
if "area" not in st.session_state:
    st.session_state["area"] = None
if "country" not in st.session_state:
    st.session_state["country"] = None
if "month" not in st.session_state:
    st.session_state["month"] = None
if "touchengmode" not in st.session_state:
    st.session_state["touchengmode"] = None
if "erchengfulfilltype" not in st.session_state:
    st.session_state["erchengfulfilltype"] = None
if "erchengmode" not in st.session_state:
    st.session_state["erchengmode"] = None
if "isbusy" not in st.session_state:
    st.session_state["isbusy_cn"] = None



col1, col2= st.columns(2)
with col1:
    st.session_state["username"]=st.selectbox('用户',get_user())
with col2:
    st.session_state["tempname"]=st.selectbox('模板',get_temp(st.session_state["username"]))
if st.session_state["username"] and st.session_state["tempname"]:

    tempdict=get_one_temp(st.session_state["username"],st.session_state["tempname"])
    st.session_state["platform"] = tempdict['platform']
    st.session_state["area"] = tempdict['area']
    st.session_state["country"] = tempdict['country']
    st.session_state["month"] = tempdict['month']
    st.session_state["touchengmode"] = tempdict['touchengmode']
    st.session_state["erchengfulfilltype"] = tempdict['erchengfulfilltype']
    st.session_state["erchengmode"] = tempdict['erchengmode']
    st.session_state["isbusy_cn"] = tempdict['isbusy1']


    col1, col2,col3,col4= st.columns(4)
    with col1:
        platform=st.selectbox('平台', [st.session_state["platform"]]+get_platform())
        area=st.selectbox('区域',[st.session_state["area"]]+get_area())
        # area=st.selectbox('区域',['US'])
    with col2:
        country=st.selectbox('国家',[st.session_state["country"]]+get_country(area))
        month=st.selectbox('月份',[st.session_state["month"]]+get_month())
    with col3:
        touchengmode=st.selectbox('头程模式',[st.session_state["touchengmode"]]+get_touchengmode(area))
        erchengfulfilltype=st.selectbox('二程发货方式',[st.session_state["erchengfulfilltype"]]+['fba','fbm'])
    with col4:
        erchengmode=st.selectbox('二程类型',[st.session_state["erchengmode"]]+get_erchengmode(platform,area,erchengfulfilltype))
        isbusy=st.selectbox('运费方式',[st.session_state["isbusy_cn"]]+['高峰','非高峰'])
        # isbusydict={'高峰':'busy','非高峰':'notbusy'}
        # isbusy1=isbusydict[isbusy]
else:
    col1, col2,col3,col4= st.columns(4)
    with col1:
        platform=st.selectbox('平台', get_platform())
        area=st.selectbox('区域',get_area())
        # area=st.selectbox('区域',['US'])
    with col2:
        country=st.selectbox('国家',get_country(area))
        month=st.selectbox('月份',get_month())
    with col3:
        touchengmode=st.selectbox('头程模式',get_touchengmode(area))
        erchengfulfilltype=st.selectbox('二程发货方式',['fba','fbm'])
    with col4:
        erchengmode=st.selectbox('二程类型',get_erchengmode(platform,area,erchengfulfilltype))
        isbusy=st.selectbox('运费方式',['高峰','非高峰'])
        # isbusydict={'高峰':'busy','非高峰':'notbusy'}
        # isbusy1=isbusydict[isbusy]

col1, col2= st.columns(2)
with col1:

    erpsku=st.text_input('erpsku')
with col2:
    usesku=st.text_input('使用sku')

# my_modal = Modal(title="", key="modal_key", max_width=400)

if "savetemp" not in st.session_state:
    st.session_state["savetemp"] = False

def save_btn_click():
    st.session_state["savetemp"] = True
def cancel_btn_click():
    st.session_state["savetemp"] = False

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
        st.write('')
        if not st.session_state['savetemp']:
            st.button('保存模板' if not st.session_state['tempname'] else '更新模板',on_click=save_btn_click)
        else:
            if st.button("取消", on_click=cancel_btn_click):
                st.experimental_rerun()

    if st.session_state['savetemp']:
        # 定义一个确定按钮，注意key值为指定的session_state，on_click调用回调函数改session_state的值
        if st.session_state['username'] and st.session_state['tempname']:
            # print(st.session_state['username'] , st.session_state['tempname'])
            updatetempindb(platform, area, country, month, touchengmode, erchengfulfilltype, erchengmode,
                                isbusy, st.session_state["username"], st.session_state["tempname"])
            st.session_state["savetemp"] = False
            st.experimental_rerun()
        else:
            with st.form("temp_form"):
                st.markdown("""保存模板
                               """)  # 这里的t[1]为用例名称
                if not st.session_state["username"]:
                    st.session_state["username"]=st.text_input('username')


                st.session_state["tempname"]=st.text_input('模板名')


                submitted =st.form_submit_button("保存")
                if submitted :
                    savetemp2db(platform, area, country, month, touchengmode, erchengfulfilltype, erchengmode,
                                isbusy, st.session_state["username"], st.session_state["tempname"])
                    st.session_state["savetemp"] = False  # 恢复session_state为False

                    st.experimental_rerun()
                # st.button("保存", key="savetemp", on_click=save_btn_click)




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

        df_s1 = st.data_editor(df_s1, hide_index=True)

        # if st.button('清空', key=1):
        #     df_s1['erp_sku']=''
        #     df_s1['广告投放']=None
        #     df_s1['预计定价']=None
        #     df_s1['数量']=1
        # else:
        #     df_s1 = st.data_editor(df_s1, hide_index=True)


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
        df_m.rename(columns={'usesku':'使用sku','platform':'平台','area':'区域','country':'国家','height':'高','lenth':'长','width':'宽','uv':'体积','expansion_rate':'膨胀系数','discount_rate':'折扣比例','exchange_rate':'汇率','purchaseprice':'采购价','purchaseprice_o':'采购价_原币','transinv_fee':'转仓费','transinv_fee_act':'转仓费_实际','invfee_rate':'仓租费率','invfee':'仓租',
                             'ercheng':'二程','ercheng_act':'二程_实际','rate_combine':'合并费率'},inplace=True)
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






st.session_state['excelfilepath']=None
st.session_state['excelfilename']=None
def outputexcel(df_m,username,tempname):
    df_out=df_m
    # df_out.to_excel('df_out.xlsx')
    filename=f'''{username}_{tempname}.xlsx'''
    filepath=f'tempfiles\\priceset\\{filename}'
    writer = pd.ExcelWriter(filepath, engine='openpyxl')
    df_out.to_excel(writer, sheet_name='Sheet1', index=False)
    workbook = writer.book
    worksheet = writer.sheets['Sheet1']

    worksheet[f'ab2'] = f'=(z2-(p2+q2+r2+u2+w2)-(x2/100+y2/100)*z2)*aa2'
    worksheet[f'ac2'] = f'=ab2/(z2*aa2)'
    worksheet[f'ad2'] = f'=(p2+q2+r2+u2+w2)/(1-(y2/100+x2/100))'
    worksheet[f'ae2'] = f'=(p2+q2+r2+u2+w2)/(1-(0.05+y2/100+x2/100))'
    worksheet[f'af2'] = f'=(p2+q2+r2+u2+w2)/(1-(0.1+y2/100+x2/100))'
    # 保存 Excel 文件
    writer._save()
    st.session_state['excelfilepath']=filepath
    st.session_state['excelfilename'] = filename
st.session_state['fileok']=False



col1, col2,col3,col4= st.columns(4)
with col1:
    if st.button('生成excel'):

        outputexcel(df_m,st.session_state['username'],st.session_state['tempname'])
        st.session_state['fileok']=True
with col2:
    if st.session_state['fileok']:
        with open(st.session_state['excelfilepath'], "rb") as file:
            btn = st.download_button(
                label="下载excel",
                data=file,
                file_name=st.session_state['excelfilename'],
                mime="application/vnd.ms-excel",
            )
st.divider()
with st.expander('测试sku填写'):
    df_tests=get_testitems()
    df_tests=st.data_editor(df_tests,hide_index=True)
    if st.button('提交'):
        res=update_testitems(df_tests)
        # st.write(res)
        if res==1:
            st.write('提交成功')
        else:
            st.write('提交失败')
