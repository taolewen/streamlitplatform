import streamlit as st

from analyzelogics.multichannelreport import dataextract_wf_order_shipped, dataextract_wf_advertise, \
    dataextract_wf_remittance, dataextract_wf_logisticsinvoice, dataextract_wf_curcharges, dataextract_wm_order, \
    dataextract_wm_ad, dataextract_wm_order_jd, dataextract_wm_payment, dataextract_wm_return, \
    dataextract_wm_settlement, dataextract_cd_orderextract, dataextract_cd_paymentdetail, dataextract_ebay_trans, \
    dataextract_ebay_orders, dataextract_mm_order, dataextract_mm_ad, dataextract_mm_return, dataextract_mm_payment, \
    dataextract_shopify_order, dataextract_shopify_ad1, dataextract_shopify_ad2
from analyzelogics.multichannelreport.attr_get import get_option

if "channel" not in st.session_state:
    st.session_state["channel"] = None
if "reporttype" not in st.session_state:
    st.session_state["reporttype"] = None
if "area" not in st.session_state:
    st.session_state["area"] = None
if "country" not in st.session_state:
    st.session_state["country"] = None
if "store" not in st.session_state:
    st.session_state["store"] = None
if "week" not in st.session_state:
    st.session_state["week"] = None
if "qijian" not in st.session_state:
    st.session_state["qijian"] = None

col1,col2=st.columns(2)
with col1:
    # st.session_state["week"]=st.selectbox('week',get_option(st.session_state["channel"])['week'])
    st.session_state["week"]=st.text_input('week')

with col2:
    # st.session_state["qijian"]=st.selectbox('qijian',get_option(st.session_state["channel"])['qijian'])
    st.session_state["qijian"]=st.text_input('qijian')


if st.session_state["week"]:
    try:
        if not (len(str(st.session_state['week'])) == 8 or len(str(st.session_state['week'])) == 6):
            raise 'week请输入6或8位整数,例：202306或20230702'
        st.session_state['week'] = int(st.session_state['week'])
    except Exception  as e:
        st.write(e)
        # Multiline_txt = values['multiline'] + '\n' + str('week请输入6或8位整数,例：202306或20230702')
        # window.Element('multiline').Update(Multiline_txt)

if st.session_state["qijian"]:
    try:
        if not (len(str(st.session_state['qijian'])) == 7 ):
            raise 'qijian请输入YYYY-mm格式,例：2023-06'
    except Exception as e:
        st.write(e)
col1,col2,col3,col4=st.columns(4)
with col1:
    st.session_state["channel"]=st.selectbox('渠道',['Ebay', 'CD', 'manomano', 'Wayfair', 'walmart','独立站'])
    st.session_state["store"]=st.selectbox('store',get_option(st.session_state["channel"])['store'])

with col2:
    st.session_state["reporttype"]=st.selectbox('报表类型',get_option(st.session_state["channel"])['reporttype'])

with col3:
    st.session_state["area"]=st.selectbox('Area',get_option(st.session_state["channel"])['area'])


with col4:
    st.session_state["country"]=st.selectbox('country',get_option(st.session_state["channel"])['country'])


uploadfile=st.file_uploader('选择文件')
if uploadfile:
    st.write(type(uploadfile))
    st.write(uploadfile)

    #检查
    pass

    ###
    if st.button('传输'):
        d={
        'area': st.session_state['area'],
        'country':st.session_state['country'],
        'store':st.session_state['store'],
        'week':st.session_state['week'],
        'qijian':st.session_state['qijian']
        # 'importdate':datetime.now().strftime('%Y-%m-%d')
        }
        if st.session_state['channel'] == 'Wayfair':
            if st.session_state['reporttype'] == '已发货订单':
                s, m = dataextract_wf_order_shipped.dealsinglefile(uploadfile, d)
            if st.session_state['reporttype'] == '广告':
                s, m = dataextract_wf_advertise.dealsinglefile(uploadfile, d)
            if st.session_state['reporttype'] == '付款':
                s, m = dataextract_wf_remittance.dealsinglefile(uploadfile, d)
            if st.session_state['reporttype'] == '物流发票':
                s, m = dataextract_wf_logisticsinvoice.dealsinglefile(uploadfile, d)
            if st.session_state['reporttype'] == 'CG发货订单':
                s, m = dataextract_wf_curcharges.dealsinglefile(uploadfile, d)
        if st.session_state['channel'] == 'walmart':
            if st.session_state['reporttype'] == '订单':
                s, m = dataextract_wm_order.dealsinglefile(uploadfile, d)
            if st.session_state['reporttype'] == '广告':
                s, m = dataextract_wm_ad.dealsinglefile(uploadfile, d)
            if st.session_state['reporttype'] == '订单—jd':
                s, m = dataextract_wm_order_jd.dealsinglefile(uploadfile, d)
            if st.session_state['reporttype'] == '付款payment':
                s, m = dataextract_wm_payment.dealsinglefile(uploadfile, d)
            if st.session_state['reporttype'] == '退货':
                s, m = dataextract_wm_return.dealsinglefile(uploadfile, d)
            if st.session_state['reporttype'] == '结算(运费)':
                s, m = dataextract_wm_settlement.dealsinglefile(uploadfile, d)
        if st.session_state['channel'] == 'CD':
            if st.session_state['reporttype'] == 'orderextract':
                s, m = dataextract_cd_orderextract.dealsinglefile(uploadfile, d)
            if st.session_state['reporttype'] == 'paymentdetail':
                s, m = dataextract_cd_paymentdetail.dealsinglefile(uploadfile, d)

        if st.session_state['channel'] == 'Ebay':
            if st.session_state['reporttype'] == 'transaction':
                s, m = dataextract_ebay_trans.dealsinglefile(uploadfile, d)
            if st.session_state['reporttype'] == 'orders':
                s, m = dataextract_ebay_orders.dealsinglefile(uploadfile, d)
        if st.session_state['channel'] == 'manomano':
            if st.session_state['reporttype'] == '订单':
                s, m = dataextract_mm_order.dealsinglefile(uploadfile, d)
            if st.session_state['reporttype'] == '广告':
                s, m = dataextract_mm_ad.dealsinglefile(uploadfile, d)
            if st.session_state['reporttype'] == '退款':
                s, m = dataextract_mm_return.dealsinglefile(uploadfile, d)
            if st.session_state['reporttype'] == '回款':
                s, m = dataextract_mm_payment.dealsinglefile(uploadfile, d)
        if st.session_state['channel'] == '独立站':
            if st.session_state['reporttype'] == '订单':
                s, m = dataextract_shopify_order.dealsinglefile(uploadfile, d)
            if st.session_state['reporttype'] == '广告1_google':
                s, m = dataextract_shopify_ad1.dealsinglefile(uploadfile, d)
            if st.session_state['reporttype'] == '广告2_bing':
                s, m = dataextract_shopify_ad2.dealsinglefile(uploadfile, d)
        if s == 1:
            Multiline_txt = values['multiline'] + '\n' + str('导入成功---' + str(d) + '---' + values['input'])
            window.Element('multiline').Update(Multiline_txt)

        elif s == 2:
            Multiline_txt = values['multiline'] + '\n' + str('导入失败' + '---' + m)
            window.Element('multiline').Update(Multiline_txt)













