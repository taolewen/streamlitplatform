import pandas as pd
import numpy as np
df_order=pd.read_excel(r'C:\Users\Administrator\Desktop\TSET111\日期范围报告-Order.xls')
df_hcf=pd.read_excel(r'C:\Users\Administrator\Desktop\TSET111\HCFulfillmentOrder列表525 - 副本.xlsx')
df_order=df_order.groupby(['order id','sku','类型','Customer']).agg(quantity=('quantity',np.sum)).reset_index()
df_hcf.rename(columns={'创建自订单':'order id'},inplace=True)
df_merge=pd.merge(df_order,df_hcf,on=['order id'],how='left')
df_merge.to_csv('df_merge.csv')