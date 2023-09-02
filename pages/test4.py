import datetime

import streamlit as st
import time

# progress_text = "Operation in progress. Please wait."
# my_bar = st.progress(0, text=progress_text)
#
# for percent_complete in range(100):
#     time.sleep(0.1)
#     my_bar.progress(percent_complete + 1, text=progress_text)
print(datetime.datetime.strptime('27/11/2022 18:54:53','%d/%m/%Y %H:%M:%S').strftime('%Y-%m-%d'))