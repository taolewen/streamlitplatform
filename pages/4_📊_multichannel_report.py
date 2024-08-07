import json
import os
import uuid

import streamlit as st
import tempfile

from analyzelogics.multichannelreport import dataextract_wf_order_shipped, dataextract_wf_advertise, \
    dataextract_wf_remittance, dataextract_wf_remittance_new, dataextract_wf_logisticsinvoice, \
    dataextract_wf_touchenginvoice, \
    dataextract_wf_curcharges, dataextract_wm_order, \
    dataextract_wm_ad, dataextract_wm_order_jd, dataextract_wm_payment, dataextract_wm_return, \
    dataextract_wm_settlement, dataextract_cd_orderextract, dataextract_cd_paymentdetail_returnmoney, \
    dataextract_ebay_trans, \
    dataextract_ebay_orders, dataextract_mm_order, dataextract_mm_ad, dataextract_mm_return, dataextract_mm_payment, \
    dataextract_shopify_order, dataextract_shopify_ad1, dataextract_shopify_ad2, \
    dataextract_cd_paymentdetail_settlement, dataextract_shein_order, dataextract_shein_payment, \
    dataextract_mm_storagefeesmf, dataextract_mm_mastersheetmf, dataextract_os_orders, dataextract_os_payment, \
    dataextract_temu_orders, dataextract_temu_billdetails, dataextract_temu_settled, \
    dataextract_djy_order, dataextract_djy_fees, dataextract_djy_disbursement, dataextract_djy_platformfees, \
    dataextract_djy_balance, dataextract_wm_platstorerent
from analyzelogics.multichannelreport.attr_get import get_option, get_week, get_qijian, get_periodrange

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
if "periodrange" not in st.session_state:
    st.session_state["periodrange"] = None
if "week" not in st.session_state:
    st.session_state["week"] = None
if "qijian" not in st.session_state:
    st.session_state["qijian"] = None
if "addcoldict" not in st.session_state:
    st.session_state["addcoldict"] = None



col1,col2,col3=st.columns(3)

with col1:
    st.session_state["week"]=st.selectbox('week',get_week()['week'])
    # st.session_state["week"]=st.text_input('week')
with col2:
    st.session_state["periodrange"]=st.text_input('periodrange',value=None if not st.session_state["week"] else get_periodrange(st.session_state["week"]),disabled=True)
with col3:
    # st.session_state["qijian"]=st.selectbox('qijian',get_option(st.session_state["channel"])['qijian'])
    st.session_state["qijian"]=st.text_input('qijian',value=None if not st.session_state["week"] else get_qijian(st.session_state["week"]),disabled=True)


# if st.session_state["week"]:
#     try:
#         if not (len(str(st.session_state['week'])) == 8 or len(str(st.session_state['week'])) == 6):
#             raise 'week请输入6或8位整数,例：202306或20230702'
#         st.session_state['week'] = int(st.session_state['week'])
#     except Exception  as e:
#         st.write(e)


if st.session_state["qijian"]:
    try:
        if not (len(str(st.session_state['qijian'])) == 7 and str(st.session_state['qijian'])[4]=='-'):
            raise 'qijian请输入YYYY-mm格式,例：2023-06'
    except Exception as e:
        st.write(e)
col1,col2,col3,col4=st.columns(4)
with col1:
    st.session_state["channel"]=st.selectbox('渠道',['Ebay', 'CD', 'manomano', 'Wayfair', 'walmart','独立站','shein','overstock','temu','djy'])
    st.session_state["store"]=st.selectbox('store',get_option(st.session_state["channel"])['store'])

with col2:
    st.session_state["reporttype"]=st.selectbox('报表类型',get_option(st.session_state["channel"])['reporttype'])
    st.write('')
with col3:
    st.session_state["area"]=st.selectbox('Area',get_option(st.session_state["channel"])['area'])

with col4:
    st.session_state["country"]=st.selectbox('country',get_option(st.session_state["channel"])['country'])

def clearjson_btn_click():
    st.session_state["addcoldict"] = None
if st.checkbox('添加额外字段',on_change=clearjson_btn_click):
    try:
        addcoljson=st.text_input('示例：{"shop":"XXXX","xxxxxx":"xxxxx"}',placeholder='{"store":"XXXX","xxxxxx":"xxxxx"}')
        if addcoljson:
            st.session_state['addcoldict']=json.loads(addcoljson)
            for key in st.session_state['addcoldict'].keys():
                st.write(f'''添加字段 {key}=>{st.session_state['addcoldict'][key]}''')
    except:
        st.error(str('请输入符合示例格式的字符串🚨🚨🚨' + '--->' + '{"store":"MM-2-MM","xxxxxx":"xxxxx"}'), icon="🚨")

# st.write(st.session_state['addcoldict'])
tab1,tab2=st.tabs(['导入','记录'])
with tab1:
    def save_uploaded_file(uploadedfile):

        path=os.path.join("tempfiles",os.path.join("multichannelreport",str(uuid.uuid4())+'_'+uploadedfile.name))
        with open(path,"wb") as f:
            f.write(uploadedfile.getbuffer())
        print(f'save file {path}')
        return path
    def delete_uploaded_file(uploadedfilepath):
        print(uploadedfilepath)
        os.remove(uploadedfilepath)
        print(f'deleted file {uploadedfilepath}')

    uploadfile=st.file_uploader('选择文件')
    if uploadfile:
        # st.write(type(uploadfilepath))
        # st.write(uploadfilepath)
        #检查

        ###
        if st.button('传输'):
            uploadfilepath = save_uploaded_file(uploadfile)

            d={
            'area': st.session_state['area'],
            'country':st.session_state['country'],
            'store':st.session_state['store'],
            'week':st.session_state['week'],
            'qijian':st.session_state['qijian'],
            'addcoldict':st.session_state['addcoldict']
            # 'importdate':datetime.now().strftime('%Y-%m-%d')
            }
            if st.session_state['channel'] == 'Wayfair':
                if st.session_state['reporttype'] == '已发货订单':
                    s, m = dataextract_wf_order_shipped.dealsinglefile(uploadfilepath, d)
                if st.session_state['reporttype'] == '广告':
                    s, m = dataextract_wf_advertise.dealsinglefile(uploadfilepath, d)
                if st.session_state['reporttype'] == '付款':
                    s, m = dataextract_wf_remittance.dealsinglefile(uploadfilepath, d)
                if st.session_state['reporttype'] == '付款_新':
                    s, m = dataextract_wf_remittance_new.dealsinglefile(uploadfilepath, d)
                if st.session_state['reporttype'] == '仓租&配送费invoice':
                    s, m = dataextract_wf_logisticsinvoice.dealsinglefile(uploadfilepath, d)
                if st.session_state['reporttype'] == '头程invoice':
                    s, m = dataextract_wf_touchenginvoice.dealsinglefile(uploadfilepath, d)
                if st.session_state['reporttype'] == 'CG发货订单':
                    s, m = dataextract_wf_curcharges.dealsinglefile(uploadfilepath, d)
            if st.session_state['channel'] == 'walmart':
                if st.session_state['reporttype'] == '订单':
                    s, m = dataextract_wm_order.dealsinglefile(uploadfilepath, d)
                if st.session_state['reporttype'] == '广告':
                    s, m = dataextract_wm_ad.dealsinglefile(uploadfilepath, d)
                if st.session_state['reporttype'] == '订单—jd':
                    s, m = dataextract_wm_order_jd.dealsinglefile(uploadfilepath, d)
                if st.session_state['reporttype'] == '付款payment':
                    s, m = dataextract_wm_payment.dealsinglefile(uploadfilepath, d)
                if st.session_state['reporttype'] == '退货':
                    s, m = dataextract_wm_return.dealsinglefile(uploadfilepath, d)
                if st.session_state['reporttype'] == '结算(运费)':
                    s, m = dataextract_wm_settlement.dealsinglefile(uploadfilepath, d)
                if st.session_state['reporttype'] == '平台仓租':
                    s, m = dataextract_wm_platstorerent.dealsinglefile(uploadfilepath, d)
            if st.session_state['channel'] == 'CD':
                if st.session_state['reporttype'] == 'orderextract':
                    s, m = dataextract_cd_orderextract.dealsinglefile(uploadfilepath, d)
                if st.session_state['reporttype'] == 'paymentdetail_回款':
                    s, m = dataextract_cd_paymentdetail_returnmoney.dealsinglefile(uploadfilepath, d)
                if st.session_state['reporttype'] == 'paymentdetail_结算':
                    s, m = dataextract_cd_paymentdetail_settlement.dealsinglefile(uploadfilepath, d)
            if st.session_state['channel'] == 'Ebay':
                if st.session_state['reporttype'] == 'transaction':
                    s, m = dataextract_ebay_trans.dealsinglefile(uploadfilepath, d)
                if st.session_state['reporttype'] == 'orders':
                    s, m = dataextract_ebay_orders.dealsinglefile(uploadfilepath, d)
            if st.session_state['channel'] == 'manomano':
                if st.session_state['reporttype'] == '订单':
                    s, m = dataextract_mm_order.dealsinglefile(uploadfilepath, d)
                if st.session_state['reporttype'] == '广告':
                    s, m = dataextract_mm_ad.dealsinglefile(uploadfilepath, d)
                if st.session_state['reporttype'] == '退款':
                    s, m = dataextract_mm_return.dealsinglefile(uploadfilepath, d)
                if st.session_state['reporttype'] == '回款':
                    s, m = dataextract_mm_payment.dealsinglefile(uploadfilepath, d)
                if st.session_state['reporttype'] == '平台仓租':
                    s, m = dataextract_mm_storagefeesmf.dealsinglefile(uploadfilepath, d)
                if st.session_state['reporttype'] == '平台物流费':
                    s, m = dataextract_mm_mastersheetmf.dealsinglefile(uploadfilepath, d)
            if st.session_state['channel'] == '独立站':
                if st.session_state['reporttype'] == '订单':
                    s, m = dataextract_shopify_order.dealsinglefile(uploadfilepath, d)
                if st.session_state['reporttype'] == '广告1_google':
                    s, m = dataextract_shopify_ad1.dealsinglefile(uploadfilepath, d)
                if st.session_state['reporttype'] == '广告2_bing':
                    s, m = dataextract_shopify_ad2.dealsinglefile(uploadfilepath, d)
            if st.session_state['channel'] == 'shein':
                if st.session_state['reporttype'] == '订单':
                    s, m = dataextract_shein_order.dealsinglefile(uploadfilepath, d)
                if st.session_state['reporttype'] == '回款':
                    s, m = dataextract_shein_payment.dealsinglefile(uploadfilepath, d)
            if st.session_state['channel'] == 'overstock':
                if st.session_state['reporttype'] == '订单':
                    s, m = dataextract_os_orders.dealsinglefile(uploadfilepath, d)
                if st.session_state['reporttype'] == '回款':
                    s, m = dataextract_os_payment.dealsinglefile(uploadfilepath, d)
            if st.session_state['channel'] == 'temu':
                if st.session_state['reporttype'] == '订单':
                    s, m = dataextract_temu_orders.dealsinglefile(uploadfilepath, d)
                if st.session_state['reporttype'] == '账单明细':
                    s, m = dataextract_temu_billdetails.dealsinglefile(uploadfilepath, d)
                if st.session_state['reporttype'] == '结算':
                    s, m = dataextract_temu_settled.dealsinglefile(uploadfilepath, d)
            if st.session_state['channel'] == 'djy':
                if st.session_state['reporttype'] == '订单汇总':
                    s, m = dataextract_djy_order.dealsinglefile(uploadfilepath, d)
                if st.session_state['reporttype'] == '费用项汇总':
                    s, m = dataextract_djy_fees.dealsinglefile(uploadfilepath, d)
                if st.session_state['reporttype'] == '平台费汇总':
                    s, m = dataextract_djy_platformfees.dealsinglefile(uploadfilepath, d)
                if st.session_state['reporttype'] == '垫付费汇总':
                    s, m = dataextract_djy_disbursement.dealsinglefile(uploadfilepath, d)
                if st.session_state['reporttype'] == '余额明细':
                    s, m = dataextract_djy_balance.dealsinglefile(uploadfilepath, d)


            print(s,m)
            delete_uploaded_file(uploadfilepath)
            if s == 1:

                st.success(str('导入成功🎉💯🎉---' + str(d)),icon="✅")

            elif s == 2:

                st.error(str('导入失败🚨🚨🚨' + '---' + m), icon="🚨")
with tab2:
    d = {
        'area': st.session_state['area'],
        'country': st.session_state['country'],
        'store': st.session_state['store'],
        'week': st.session_state['week'],
        'qijian': st.session_state['qijian']
        # 'importdate':datetime.now().strftime('%Y-%m-%d')
    }
    if st.session_state['channel'] == 'Wayfair':
        if st.session_state['reporttype'] == '已发货订单':
            df_check=dataextract_wf_order_shipped.selectbatch(d)
        elif st.session_state['reporttype'] == '广告':
            df_check=dataextract_wf_advertise.selectbatch(d)
        elif st.session_state['reporttype'] == '付款':
            df_check=dataextract_wf_remittance.selectbatch(d)
        elif st.session_state['reporttype'] == '付款_新':
            df_check=dataextract_wf_remittance_new.selectbatch(d)
        elif st.session_state['reporttype'] == '仓租&配送费invoice':
            df_check=dataextract_wf_logisticsinvoice.selectbatch(d)
        elif st.session_state['reporttype'] == '头程invoice':
            df_check = dataextract_wf_touchenginvoice.selectbatch(d)
        elif st.session_state['reporttype'] == 'CG发货订单':
            df_check=dataextract_wf_curcharges.selectbatch(d)
    elif st.session_state['channel'] == 'walmart':
        if st.session_state['reporttype'] == '订单':
            df_check=dataextract_wm_order.selectbatch(d)
        elif st.session_state['reporttype'] == '广告':
            df_check=dataextract_wm_ad.selectbatch(d)
        elif st.session_state['reporttype'] == '订单—jd':
            df_check=dataextract_wm_order_jd.selectbatch(d)
        elif st.session_state['reporttype'] == '付款payment':
            df_check=dataextract_wm_payment.selectbatch(d)
        elif st.session_state['reporttype'] == '退货':
            df_check=dataextract_wm_return.selectbatch(d)
        elif st.session_state['reporttype'] == '结算(运费)':
            df_check=dataextract_wm_settlement.selectbatch(d)
        if st.session_state['reporttype'] == '平台仓租':
            df_check=dataextract_wm_platstorerent.selectbatch(d)
    elif st.session_state['channel'] == 'CD':
        if st.session_state['reporttype'] == 'orderextract':
            df_check=dataextract_cd_orderextract.selectbatch(d)
        elif st.session_state['reporttype'] == 'paymentdetail_回款':
            df_check=dataextract_cd_paymentdetail_returnmoney.selectbatch(d)
        elif st.session_state['reporttype'] == 'paymentdetail_结算':
            df_check=dataextract_cd_paymentdetail_settlement.selectbatch(d)
    elif st.session_state['channel'] == 'Ebay':
        if st.session_state['reporttype'] == 'transaction':
            df_check=dataextract_ebay_trans.selectbatch(d)
        elif st.session_state['reporttype'] == 'orders':
            df_check=dataextract_ebay_orders.selectbatch(d)
    elif st.session_state['channel'] == 'manomano':
        if st.session_state['reporttype'] == '订单':
            df_check=dataextract_mm_order.selectbatch(d)
        elif st.session_state['reporttype'] == '广告':
            df_check=dataextract_mm_ad.selectbatch(d)
        elif st.session_state['reporttype'] == '退款':
            df_check=dataextract_mm_return.selectbatch(d)
        elif st.session_state['reporttype'] == '回款':
            df_check=dataextract_mm_payment.selectbatch(d)
        elif st.session_state['reporttype'] == '平台仓租':
            df_check=dataextract_mm_storagefeesmf.selectbatch(d)
        elif st.session_state['reporttype'] == '平台物流费':
            df_check=dataextract_mm_mastersheetmf.selectbatch(d)
    elif st.session_state['channel'] == '独立站':
        if st.session_state['reporttype'] == '订单':
            df_check=dataextract_shopify_order.selectbatch(d)
        elif st.session_state['reporttype'] == '广告1_google':
            df_check=dataextract_shopify_ad1.selectbatch(d)
        elif st.session_state['reporttype'] == '广告2_bing':
            df_check=dataextract_shopify_ad2.selectbatch(d)
    elif st.session_state['channel'] == 'shein':
        if st.session_state['reporttype'] == '订单':
            df_check=dataextract_shein_order.selectbatch(d)
        elif st.session_state['reporttype'] == '回款':
            df_check=dataextract_shein_payment.selectbatch(d)
    elif st.session_state['channel'] == 'overstock':
        if st.session_state['reporttype'] == '订单':
            df_check=dataextract_os_orders.selectbatch(d)
        elif st.session_state['reporttype'] == '回款':
            df_check=dataextract_os_payment.selectbatch(d)
    elif st.session_state['channel'] == 'temu':
        if st.session_state['reporttype'] == '订单':
            df_check=dataextract_temu_orders.selectbatch(d)
        if st.session_state['reporttype'] == '账单明细':
            df_check=dataextract_temu_billdetails.selectbatch(d)
        if st.session_state['reporttype'] == '结算':
            df_check=dataextract_temu_settled.selectbatch(d)
    if st.session_state['channel'] == 'djy':
        if st.session_state['reporttype'] == '订单汇总':
            df_check = dataextract_djy_order.selectbatch(d)
        if st.session_state['reporttype'] == '费用项汇总':
            df_check = dataextract_djy_fees.selectbatch(d)
        if st.session_state['reporttype'] == '平台费汇总':
            df_check = dataextract_djy_platformfees.selectbatch(d)
        if st.session_state['reporttype'] == '垫付费汇总':
            df_check = dataextract_djy_disbursement.selectbatch(d)
        if st.session_state['reporttype'] == '余额明细':
            df_check = dataextract_djy_balance.selectbatch(d)

    df_delete=st.data_editor(df_check,    column_config={
        "delete": st.column_config.CheckboxColumn(
            "是否删除",
            help="勾选要删除的已导入报表",
            default=False,
        )
    },
    disabled=["batchid","area","country","qijian","week","store","filename","createdate","reporttype"],
    hide_index=True,)

    if "delete" not in st.session_state:
        st.session_state["delete"] = False


    def delete_btn_click():
        st.session_state["delete"] = True


    def canceldelete_btn_click():
        st.session_state["delete"] = False


    def delete_by_batchid(batchid):
        if st.session_state['channel'] == 'Wayfair':
            if st.session_state['reporttype'] == '已发货订单':
                s, m = dataextract_wf_order_shipped.deletebatch(batchid)
            if st.session_state['reporttype'] == '广告':
                s, m = dataextract_wf_advertise.deletebatch(batchid)
            if st.session_state['reporttype'] == '付款':
                s, m = dataextract_wf_remittance.deletebatch(batchid)
            if st.session_state['reporttype'] == '付款_新':
                s, m = dataextract_wf_remittance_new.deletebatch(batchid)
            if st.session_state['reporttype'] == '仓租&配送费invoice':
                s, m = dataextract_wf_logisticsinvoice.deletebatch(batchid)
            if st.session_state['reporttype'] == '头程invoice':
                s, m = dataextract_wf_touchenginvoice.deletebatch(batchid)
            if st.session_state['reporttype'] == 'CG发货订单':
                s, m = dataextract_wf_curcharges.deletebatch(batchid)
        if st.session_state['channel'] == 'walmart':
            if st.session_state['reporttype'] == '订单':
                s, m = dataextract_wm_order.deletebatch(batchid)
            if st.session_state['reporttype'] == '广告':
                s, m = dataextract_wm_ad.deletebatch(batchid)
            if st.session_state['reporttype'] == '订单—jd':
                s, m = dataextract_wm_order_jd.deletebatch(batchid)
            if st.session_state['reporttype'] == '付款payment':
                s, m = dataextract_wm_payment.deletebatch(batchid)
            if st.session_state['reporttype'] == '退货':
                s, m = dataextract_wm_return.deletebatch(batchid)
            if st.session_state['reporttype'] == '结算(运费)':
                s, m = dataextract_wm_settlement.deletebatch(batchid)
            if st.session_state['reporttype'] == '平台仓租':
                s, m = dataextract_wm_platstorerent.deletebatch(batchid)

        if st.session_state['channel'] == 'CD':
            if st.session_state['reporttype'] == 'orderextract':
                s, m = dataextract_cd_orderextract.deletebatch(batchid)
            if st.session_state['reporttype'] == 'paymentdetail_回款':
                s, m = dataextract_cd_paymentdetail_returnmoney.deletebatch(batchid)
            if st.session_state['reporttype'] == 'paymentdetail_结算':
                s, m = dataextract_cd_paymentdetail_settlement.deletebatch(batchid)
        if st.session_state['channel'] == 'Ebay':
            if st.session_state['reporttype'] == 'transaction':
                s, m = dataextract_ebay_trans.deletebatch(batchid)
            if st.session_state['reporttype'] == 'orders':
                s, m = dataextract_ebay_orders.deletebatch(batchid)
        if st.session_state['channel'] == 'manomano':
            if st.session_state['reporttype'] == '订单':
                s, m = dataextract_mm_order.deletebatch(batchid)
            if st.session_state['reporttype'] == '广告':
                s, m = dataextract_mm_ad.deletebatch(batchid)
            if st.session_state['reporttype'] == '退款':
                s, m = dataextract_mm_return.deletebatch(batchid)
            if st.session_state['reporttype'] == '回款':
                s, m = dataextract_mm_payment.deletebatch(batchid)
            if st.session_state['reporttype'] == '平台仓租':
                s, m = dataextract_mm_storagefeesmf.deletebatch(batchid)
            if st.session_state['reporttype'] == '平台物流费':
                s, m = dataextract_mm_mastersheetmf.deletebatch(batchid)
        if st.session_state['channel'] == '独立站':
            if st.session_state['reporttype'] == '订单':
                s, m = dataextract_shopify_order.deletebatch(batchid)
            if st.session_state['reporttype'] == '广告1_google':
                s, m = dataextract_shopify_ad1.deletebatch(batchid)
            if st.session_state['reporttype'] == '广告2_bing':
                s, m = dataextract_shopify_ad2.deletebatch(batchid)
        if st.session_state['channel'] == 'shein':
            if st.session_state['reporttype'] == '订单':
                s, m = dataextract_shein_order.deletebatch(batchid)
            if st.session_state['reporttype'] == '回款':
                s, m = dataextract_shein_payment.deletebatch(batchid)
        if st.session_state['channel'] == 'overstock':
            if st.session_state['reporttype'] == '订单':
                s, m = dataextract_os_orders.deletebatch(batchid)
            if st.session_state['reporttype'] == '回款':
                s, m = dataextract_os_payment.deletebatch(batchid)
        if st.session_state['channel'] == 'temu':
            if st.session_state['reporttype'] == '订单':
                s, m = dataextract_temu_orders.deletebatch(batchid)
            if st.session_state['reporttype'] == '账单明细':
                s, m = dataextract_temu_billdetails.deletebatch(batchid)
            if st.session_state['reporttype'] == '结算':
                s, m = dataextract_temu_settled.deletebatch(batchid)
        if st.session_state['channel'] == 'djy':
            if st.session_state['reporttype'] == '订单汇总':
                s, m = dataextract_djy_order.deletebatch(batchid)
            if st.session_state['reporttype'] == '费用项汇总':
                s, m = dataextract_djy_fees.deletebatch(batchid)
            if st.session_state['reporttype'] == '平台费汇总':
                s, m = dataextract_djy_platformfees.deletebatch(batchid)
            if st.session_state['reporttype'] == '垫付费汇总':
                s, m = dataextract_djy_disbursement.deletebatch(batchid)
            if st.session_state['reporttype'] == '余额明细':
                s, m = dataextract_djy_balance.deletebatch(batchid)


        return s,m
    if not st.session_state['delete'] and len(df_delete.loc[df_delete['delete']==True])!=0:
        st.button('删除',on_click=delete_btn_click)
    if st.session_state['delete']:
        with st.form("deletecomferm_form"):
            st.markdown("""确定要删除以下报表吗？""")
            st.dataframe(df_delete.loc[df_delete['delete']==True])
            col1,col2,col3,col4=st.columns(4)
            with col1:
                submitted = st.form_submit_button("确认删除")
            with col2:
                cancel = st.form_submit_button('取消', on_click=canceldelete_btn_click)
            if submitted:
                # st.write(df_delete.loc[df_delete['delete']==True]['batchid'].tolist())
                for batchid in df_delete.loc[df_delete['delete']==True]['batchid'].tolist():
                    s,m=delete_by_batchid(batchid)
                st.session_state['delete']=False
                st.experimental_rerun()


