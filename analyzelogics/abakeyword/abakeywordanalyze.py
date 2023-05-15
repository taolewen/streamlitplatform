import pandas as pd


def anaylyzefile(uploadfile):
    df_data = pd.read_excel(uploadfile)
    def removedup(x):
        mset=set(x.split(' '))
        words=' '.join(mset)
        return words

    df_data['客户搜索词']=df_data['客户搜索词'].astype(str)
    df_data['客户搜索词']=df_data['客户搜索词'].apply(lambda x:removedup(x))
    df_data = df_data.drop('客户搜索词', axis=1).join(
        df_data['客户搜索词'].str.split(' ', expand=True).stack().reset_index(level=1, drop=True).rename('客户搜索词'))
    # print(df_data)
    df = df_data.groupby('客户搜索词').sum().reset_index()

    # print(df)
    return df