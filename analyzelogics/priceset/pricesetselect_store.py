import traceback
from urllib.parse import quote_plus

import numpy as np
import pandas as pd
import pymysql

# from dbs import mysqlconn
import streamlit as st
from numpy import float64
from sqlalchemy import create_engine

@st.cache_data#价格
def get_price_gplus(erchengfulfilltype,country):
    mysqlconn = pymysql.connect(host=st.secrets["mysql"]['host'],
                                user=st.secrets["mysql"]['user'],
                                password=st.secrets["mysql"]['password'],
                                db=st.secrets["mysql"]['database'])
    df_price_gplus = pd.read_sql(sql='''
            select storerela.store_country 站点,commo.country,commo.erp_sku,commo.配送渠道,commo.预计定价
            from
            (
                select 
                distinct 站点,substring_index(站点,':',-1) country,erp_sku,
                case when 配送渠道='买家自配送' then 'fbm' when 配送渠道='亚马逊配送' then 'fba' else '' end 配送渠道,
                (case when 优惠价=0 or 优惠价 is null then 原价 else 优惠价 end) 预计定价
                from
                gplus_commodity_management
                where id in (select min(id) id from gplus_commodity_management group by 站点,erp_sku,substring_index(站点,':',-1))
    		) commo
    		left join
    		(
    		    select concat(concat(store,':'),country) store,store_country from gplus_store_relate
    		) storerela
				    		on commo.站点=storerela.store

        ''',con=mysqlconn)
    df_price_gplus=df_price_gplus.loc[df_price_gplus['配送渠道']==erchengfulfilltype]
    df_price_gplus=df_price_gplus.loc[df_price_gplus['country']==country]
    df_price_gplus['预计定价']=df_price_gplus['预计定价'].astype('float64')
    df_price_gplus.drop(columns=['配送渠道'],inplace=True)
    df_price_gplus.drop(columns=['country'],inplace=True)
    return df_price_gplus

@st.cache_data#店铺
def get_price_gplus_store(erchengfulfilltype,country):
    mysqlconn = pymysql.connect(host=st.secrets["mysql"]['host'],
                                user=st.secrets["mysql"]['user'],
                                password=st.secrets["mysql"]['password'],
                                db=st.secrets["mysql"]['database'])
    df_price_gplus = pd.read_sql(sql='''    
            select storerela.store_country 站点,commo.country,commo.erp_sku,commo.配送渠道,commo.预计定价
            from
            (
                select 
                distinct 站点,substring_index(站点,':',-1) country,erp_sku,
                case when 配送渠道='买家自配送' then 'fbm' when 配送渠道='亚马逊配送' then 'fba' else '' end 配送渠道,
                (case when 优惠价=0 or 优惠价 is null then 原价 else 优惠价 end) 预计定价
                from
                gplus_commodity_management
                where id in (select min(id) id from gplus_commodity_management group by 站点,erp_sku,substring_index(站点,':',-1))
    		) commo
    		left join
    		(
    		    select concat(concat(store,':'),country) store,store_country from gplus_store_relate
    		) storerela
				    		on commo.站点=storerela.store

        ''',con=mysqlconn)
    df_price_gplus=df_price_gplus.loc[df_price_gplus['配送渠道']==erchengfulfilltype]
    df_price_gplus=df_price_gplus.loc[df_price_gplus['country']==country]
    df_price_gplus['预计定价']=df_price_gplus['预计定价'].astype('float64')
    df_price_gplus.drop(columns=['配送渠道'],inplace=True)
    df_price_gplus.drop(columns=['country'],inplace=True)
    df_price_gplus=df_price_gplus.groupby(['站点','erp_sku']).sum().reset_index()
    df=df_price_gplus[['站点','erp_sku']]
    return df

@st.cache_data#海外仓仓租
def get_storerent_oversea(country):
    mysqlconn = pymysql.connect(host=st.secrets["mysql"]['host'],
                                user=st.secrets["mysql"]['user'],
                                password=st.secrets["mysql"]['password'],
                                db=st.secrets["mysql"]['database'])
    rentfee_sql=f'''select erpsku erp_sku,store_kw 站点,round(avg(storefee_daily_per),4) invrent
    from amz_inv_aging_cal_iabl
    where create_date in
	(select max(create_date) from amz_inv_aging_cal group by left(create_date,10))
	and country ='{country}'
    group by erpsku,store_kw
    '''
    df_rentfee=pd.read_sql(rentfee_sql,con=mysqlconn)
    return df_rentfee
@st.cache_data#fba仓租
def get_storerent_fba():
    mysqlconn = pymysql.connect(host=st.secrets["mysql"]['host'],
                                user=st.secrets["mysql"]['user'],
                                password=st.secrets["mysql"]['password'],
                                db=st.secrets["mysql"]['database'])
    rentfee_sql='''
        select 站点,erp_sku,case when 销售数量!=0 then avg(月度仓储费/销售数量) else 0 end invrent 
        from date_range_report_month
        where 月份 = left(DATE_SUB(CURDATE(), INTERVAL 30 DAY),7)
        group by 站点,erp_sku
    '''
    df_rentfee=pd.read_sql(rentfee_sql,con=mysqlconn)
    return df_rentfee


@st.cache_data  # 广告
def get_spfee(country):
    mysqlconn = pymysql.connect(host=st.secrets["mysql"]['host'],
                                user=st.secrets["mysql"]['user'],
                                password=st.secrets["mysql"]['password'],
                                db=st.secrets["mysql"]['database'])
    spfee_sql = f'''
        select erp_sku,store 站点,avg(花费) adcost
        from sp
        where left(create_date,10) > DATE_SUB(CURDATE(), INTERVAL 30 DAY) and country='{country}'
        group by erp_sku,store
    '''
    df_spfee = pd.read_sql(spfee_sql, con=mysqlconn)
    return df_spfee

@st.cache_data  # 折扣
def get_discountfee():
    mysqlconn = pymysql.connect(host=st.secrets["mysql"]['host'],
                                user=st.secrets["mysql"]['user'],
                                password=st.secrets["mysql"]['password'],
                                db=st.secrets["mysql"]['database'])
    discountfee_sql = '''
        select 站点,erp_sku,-avg(折扣) discount 
        from date_range_report_month
        where left(createtime,10) > DATE_SUB(CURDATE(), INTERVAL 30 DAY)
        group by 站点,erp_sku
    '''
    df_discountfee = pd.read_sql(discountfee_sql, con=mysqlconn)
    return df_discountfee

@st.cache_data
def get_platform():
    mysqlconn = pymysql.connect(host=st.secrets["mysql"]['host'],
                                user=st.secrets["mysql"]['user'],
                                password=st.secrets["mysql"]['password'],
                                db=st.secrets["mysql"]['database'])
    df=pd.read_sql(f'''select platform from priceset_base_result group by platform''',con=mysqlconn)
    return df['platform'].to_list()


@st.cache_data
def get_area():
    mysqlconn = pymysql.connect(host=st.secrets["mysql"]['host'],
                                user=st.secrets["mysql"]['user'],
                                password=st.secrets["mysql"]['password'],
                                db=st.secrets["mysql"]['database'])
    df=pd.read_sql(f'''select area from priceset_base_result group by area''',con=mysqlconn)
    return df['area'].to_list()
@st.cache_data
def get_country(area=None):
    mysqlconn = pymysql.connect(host=st.secrets["mysql"]['host'],
                                user=st.secrets["mysql"]['user'],
                                password=st.secrets["mysql"]['password'],
                                db=st.secrets["mysql"]['database'])
    df=pd.read_sql(f'''select country from priceset_base_result 
                        where 1=1
                    {"" if not area else f"and area = '{area}'"}
                    group by country''',con=mysqlconn)
    return df['country'].to_list()
@st.cache_data
def get_month():
    mysqlconn = pymysql.connect(host=st.secrets["mysql"]['host'],
                                user=st.secrets["mysql"]['user'],
                                password=st.secrets["mysql"]['password'],
                                db=st.secrets["mysql"]['database'])
    df=pd.read_sql(f'''select 月份 month from priceset_toucheng_relate group by 月份 order by 月份 desc''',con=mysqlconn)
    return df['month'].to_list()
@st.cache_data
def get_touchengmode(area=None):
    mysqlconn = pymysql.connect(host=st.secrets["mysql"]['host'],
                                user=st.secrets["mysql"]['user'],
                                password=st.secrets["mysql"]['password'],
                                db=st.secrets["mysql"]['database'])
    df=pd.read_sql(f'''select 模式 from priceset_toucheng_relate 
                    where 1=1
                    {"" if not area else f"and 区域 = '{area}'"}
                    group by 模式''',con=mysqlconn)
    return df['模式'].to_list()
@st.cache_data
def get_erchengmode(platform=None,area=None,erchengfulfilltype=None):
    mysqlconn = pymysql.connect(host=st.secrets["mysql"]['host'],
                                user=st.secrets["mysql"]['user'],
                                password=st.secrets["mysql"]['password'],
                                db=st.secrets["mysql"]['database'])
    df=pd.read_sql(f'''

                    select ftype from 
                    (
                    SELECT 'fba' ectype,erp_sku,platform,area,country,concat('fba_',ftype) ftype,erchengfee_total_notbusy,erchengfee_total_busy FROM `priceset_fba_result`
                    ) a 
                    where 1=1
                    and ectype='{erchengfulfilltype}'
                    {"" if not platform else f"and platform = '{platform}'"}
                    {"" if not area else f"and area = '{area}'"}
                    group by ftype
                    
                    union all
                    select ftype from 
                    (
                    SELECT 'fbm' ectype,erp_sku,'None' platform,area,country,concat('fbm_',concat(delivery_merchant,channel)) ftype,erchengfee_total erchengfee_total_notbusy,erchengfee_total erchengfee_total_busy 
                    FROM `priceset_fbm_result`
                    where source='zdfc'
                    ) m
                    where 1=1
                    and ectype='{erchengfulfilltype}'
                    {"" if not area else f"and area = '{area}'"}
                    group by ftype

            ''',con=mysqlconn)
    return df['ftype'].to_list()


@st.cache_data
def get_feerate(ratename,platform=None,country=None):
    mysqlconn = pymysql.connect(host=st.secrets["mysql"]['host'],
                                user=st.secrets["mysql"]['user'],
                                password=st.secrets["mysql"]['password'],
                                db=st.secrets["mysql"]['database'])
    df=pd.read_sql(f'''select platform,country,
                        仓租系数,波动系数,佣金比率,VAT系数,其他费用比率
                        
                        from priceset_channel_feerate
                        where 1=1
                        

                    {"" if len(platform)==999 else f"and platform = '{platform}'"}
                    {"" if len(country)==999 else f"and country = '{country}'"}

                        ''',con=mysqlconn)
    if len(df[ratename].to_list())==0:
        return 0
    else:
        return (df[ratename].to_list()[0])

@st.cache_data
def cal_data(platform=None,area=None,country=None,erpsku=None,usesku=None,month=None,touchengmode=None,isbusy=None,erchengfulfilltype=None,erchengmode=None,
            invrentrate=None,commissionrate=None,vatrate=None,otherrate=None,waverate=None
             ):
    isbusydict = {'高峰': 'busy', '非高峰': 'notbusy'}
    isbusy1 = isbusydict[isbusy]
    mysqlconn = pymysql.connect(host=st.secrets["mysql"]['host'],
                                user=st.secrets["mysql"]['user'],
                                password=st.secrets["mysql"]['password'],
                                db=st.secrets["mysql"]['database'])
    df_base=pd.read_sql(f'''
                    select erp_sku,usesku,platform,area,country,height,lenth,width,uv,purchaseprice,transinv_fee,invfee invfee_rate,expansion_rate
                    from priceset_base_result
                    where 1=1
                    {"" if not platform else f"and platform = '{platform}'"}
                    {"" if not area else f"and area = '{area}'"}
                    {"" if not country else f"and country = '{country}'"}
                    {"" if not erpsku else f"and erp_sku like '%{erpsku}%'"}
                    {"" if not usesku else f"and usesku like '%{usesku}%'"}
                    
                    ''',con=mysqlconn)

    df_exchangerate=pd.read_sql(f'''
                            select country,exchange_rate from exchange_rate
                            where 1=1
                            {"" if not month else f"and left(date,7) = '{month}'"}
                    ''',con=mysqlconn)
    df_exchangerate['exchange_rate']=df_exchangerate['exchange_rate'].astype('float64')
    df_m=pd.merge(df_base,df_exchangerate,on=['country'],how='left')
    df_m['exchange_rate'].fillna(1,inplace=True)

    df_m['purchaseprice_o']=df_m.apply(lambda x:x.purchaseprice/x.exchange_rate,axis=1)

    def cal_transinvfee_act(x):#转仓费实际逻辑
        if erchengfulfilltype=='fbm':
            return 0
        elif platform=='amazon' and area == 'US' and country =='US' and touchengmode == '发货-美东' and erchengfulfilltype == 'fba' and erchengmode == 'fba_常规':
            return 0
        elif platform == 'VC_PO' or platform == 'VC_DF' or platform == 'VC_DI':
            return 0
        elif touchengmode=='WF直发':
            return 0
        elif platform=='amazon' and area == 'US' and country =='US' and touchengmode == '发货-美西' and erchengfulfilltype == 'fba' and erchengmode == 'fba_常规':
            return x+5
        else:
            return x

    df_m['transinv_fee_act']=df_m.apply(lambda x:cal_transinvfee_act(x.transinv_fee),axis=1)
    # df_m.to_csv('test111.csv')
    # print('invrentrate')
    # print(type(invrentrate))
    # df_m['invfee']=df_m.apply(lambda x:x.invfee_rate*x.uv*invrentrate,axis=1)

    df_skustore=get_price_gplus_store(erchengfulfilltype,country)
    df_m=pd.merge(df_m,df_skustore,on=['erp_sku'],how='left')
    #仓租
    if erchengfulfilltype == 'fbm':
        df_invfee=get_storerent_oversea(country)
        df_m=pd.merge(df_m,df_invfee,on=['erp_sku','站点'],how='left')
        df_m['invfee']=df_m['invrent']
        df_m.drop(columns=['invrent'], inplace=True)
    else:
        df_invfee=get_storerent_fba()
        df_m=pd.merge(df_m,df_invfee,on=['erp_sku','站点'],how='left')
        df_m['invfee']=df_m['invrent']
        df_m.drop(columns=['invrent'], inplace=True)
    #折扣discount
    df_discount = get_discountfee()
    df_m = pd.merge(df_m, df_discount, on=['erp_sku', '站点'], how='left')


    df_toucheng=pd.read_sql(f'''
                            select 区域 area,头程系数 from priceset_toucheng_relate
                            where 1=1
                            {"" if not month else f"and 月份 = '{month}'"}
                            {"" if not area else f"and 区域 = '{area}'"}
                            {"" if not touchengmode else f"and 模式 = '{touchengmode}'"}
                    ''',con=mysqlconn)
    df_m=pd.merge(df_m,df_toucheng,on=['area'],how='left')
    df_m['头程']=df_m.apply(lambda x:x.uv*x.expansion_rate*x.头程系数,axis=1)
    df_m['头程_原币']=df_m.apply(lambda x:2 if platform=='VC_DI' else x.头程/x.exchange_rate,axis=1)

    df_ercheng=pd.read_sql(f'''
                                select erp_sku,ec ercheng from 
                                (
                                SELECT 'fba' ectype,erp_sku,platform,area,country,concat('fba_',ftype) ftype,(case when '{isbusy1}' ='notbusy' then erchengfee_total_notbusy else erchengfee_total_busy end) ec 
                                FROM `priceset_fba_result`
                                ) a 
                                where ectype='{erchengfulfilltype}'
                                {"" if not platform else f"and platform = '{platform}'"}
                                {"" if not area else f"and area = '{area}'"}
                                {"" if not country else f"and country = '{country}'"}
                                {"" if not erchengmode else f"and ftype = '{erchengmode}'"}

                                union all
                                select erp_sku,ec ercheng from 
                                (
                                SELECT 'fbm' ectype,erp_sku,'None' platform,area,country,concat('fbm_',concat(delivery_merchant,channel)) ftype,
                                (case 
                                 when ('{platform}'='WF' or '{platform}' = 'VC_PO' or '{platform}' = 'VC_DF') and '{erchengmode}'='fbm_defaultdefault' and ('{area}'='US' or '{area}' = 'CA') then outstorefee+2
                                 when ('{platform}'='WF' or '{platform}' = 'VC_PO' or '{platform}' = 'VC_DF') and '{erchengmode}'='fbm_defaultdefault' and ('{area}'='EU' ) then outstorefee+1
                                 when ('{platform}' = 'VC_DI')  then 0
                                else erchengfee_total end ) ec FROM `priceset_fbm_result` where source='zdfc'
                                ) m
                                where ectype='{erchengfulfilltype}'
                                {"" if not area else f"and area = '{area}'"}
                                {"" if not country else f"and country = '{country}'"}
                                {"" if not erchengmode else f"and ftype = '{erchengmode}'"}
                    ''',con=mysqlconn)
    df_m=pd.merge(df_m,df_ercheng,on=['erp_sku'],how='left')
    df_m['ercheng_act']=df_m.apply(lambda x:0 if platform=='VC_DI' else x.ercheng,axis=1)
    df_m['rate_combine']=vatrate+waverate+commissionrate+otherrate
    df_m['rate_combine_VAT税率']=vatrate
    df_m['rate_combine_波动系数']=waverate
    df_m['rate_combine_佣金费率']=commissionrate
    df_m['rate_combine_其他费用']=otherrate
    # print(df_m)
    # df_m.to_csv('testkkk.csv')
    return df_m

@st.cache_data
def get_testitems():
    mysqlconn = pymysql.connect(host=st.secrets["mysql"]['host'],
                                user=st.secrets["mysql"]['user'],
                                password=st.secrets["mysql"]['password'],
                                db=st.secrets["mysql"]['database'])
    df=pd.read_sql(f'''select erp_sku,
                        usesku,
                        lenth,
                        width,
                        height,
                        uv,
                        grossweight,
                        packingrate,
                        purchaseprice
                         from priceset_testitem
                   ''',con=mysqlconn)
    l=len(df)
    for i in range(0+l, 50+l):
        df.loc[i] = [np.NAN, np.NAN, np.NAN, np.NAN, np.NAN, np.NAN, np.NAN, np.NAN, np.NAN]
    return df


def update_testitems(df):
    try:
        df=df.loc[  ~df.erp_sku .isnull()]
        host = st.secrets["mysql"]['host'],
        user = st.secrets["mysql"]['user'],
        password = st.secrets["mysql"]['password'],
        db = st.secrets["mysql"]['database']
        connstr = f"mysql+pymysql://{user[0]}:%s@{host[0]}:3306/{db}?charset=utf8" % quote_plus(f'{password[0]}')

        engine = create_engine(connstr)

        df.to_sql(name='priceset_testitem', con=engine, if_exists='replace', index=False, index_label=False)

        return 1
    except:
        traceback.print_exc()
        return 2
