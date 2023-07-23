import streamlit as st
import pandas as pd
import numpy as np
from dbs import  mysqlconn

st.session_state['is_runed']=0
# st.session_state['df']=None
# df['test3'] = df['test1'] + df['test2']


def returndf():
    if st.session_state['is_runed'] == 0:
        print('aaa')
        st.session_state['df'] = pd.read_sql(f'''select mailaddress,status,name,1 test1,2 test2 from email_check''', con=mysqlconn)

        st.session_state['df']['test3'] = st.session_state['df']['test1'] + st.session_state['df']['test2']
        st.session_state['is_runed'] = 1
        return st.session_state['df']
    else:
        print('bbb')
        # print(st.session_state['df'])

        # df['test3'] = df['test1'] + df['test2']
        st.session_state['df']['test3']=st.session_state['df'].apply(lambda x:x.test1+x.test2,axis=1)
        return st.session_state['df']

# editddf = st.data_editor(df)

st.session_state['df']=returndf()
print(st.session_state['df'])

print(st.session_state['is_runed'])
st.session_state['df']=st.data_editor(st.session_state['df'])
st.write(st.session_state['df'])


# editddf=st.data_editor(returndf(editddf))


