import streamlit_echarts
from pyecharts.charts import Bar
from pyecharts import options as opts
from pyecharts.globals import ThemeType
bar = Bar()
bar.add_xaxis([1,2,3,4,5,6,7])
bar.add_yaxis('jlr' ,[6,5,4,3,2,1])
streamlit_echarts.st_pyecharts(
    bar,
    theme=ThemeType.DARK
)