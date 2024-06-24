import json
import os
import uuid

import streamlit as st
from analyzelogics.data_import import dataimport

def save_uploaded_file(uploadedfile):
    path = os.path.join("tempfiles", os.path.join("dataimport", str(uuid.uuid4()) + '_' + uploadedfile.name))
    with open(path, "wb") as f:
        f.write(uploadedfile.getbuffer())
    print(f'save file {path}')
    return path

def delete_uploaded_file(uploadedfilepath):
    print(uploadedfilepath)
    os.remove(uploadedfilepath)
    print(f'deleted file {uploadedfilepath}')
uploadfile = st.file_uploader('选择文件')
if uploadfile:
    # st.write(type(uploadfilepath))
    # st.write(uploadfilepath)
    # 检查

    ###
    if st.button('传输'):
        uploadfilepath = save_uploaded_file(uploadfile)

        s, m = dataimport.dealsinglefile(uploadfilepath)

        print(s, m)
        delete_uploaded_file(uploadfilepath)
        if s == 1:

            st.success(str('导入成功🎉💯🎉---' + str('')), icon="✅")

        elif s == 2:

            st.error(str('导入失败🚨🚨🚨' + '---' + m), icon="🚨")