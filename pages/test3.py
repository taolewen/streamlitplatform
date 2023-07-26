import streamlit as st

platform = st.selectbox('平台', [1,2,3,4], key='platform')
if st.button('c'):
    st.experimental_rerun()
    st.session_state['platform']=4
