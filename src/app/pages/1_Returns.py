import plotly.express as px
import pandas as pd
from pathlib import Path
import os
import sys
import streamlit as st

# SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# sys.path.append(os.path.dirname(SCRIPT_DIR))

# print('YOYO',os.path.join(Path(__file__).parent.parent.absolute(),'db'))

# sys.path.append(os.path.join(Path(__file__).parent.parent.parent.absolute(),'db','data'))
from data import FFData, plot_configs

st.set_page_config(
    page_title="Data Table",
    layout="wide"
)


st.markdown("""
        <style>
               .block-container {
                    padding-top: 1rem;
                    padding-bottom: 0rem;
                    padding-left: 1rem;
                    padding-right: 1rem;
                }
        </style>
        """, unsafe_allow_html=True)

# st.markdown("""
#             # View the Data as a Table
#             """)

ftype = st.sidebar.selectbox("Type", options=["Factor","Portfolio","Industry"], help='Pick whether to look at factors (long-short portoflio), portflios (long-only) or Inudstry returns')

if ftype=="Factor":
    n = st.sidebar.selectbox("Number of Factors", options=[3,5],help="Which factor model to use")
elif ftype=="Portfolio":
    n = st.sidebar.selectbox("Number of Portfolios", options=[6,25],help="Which portflio to use")
elif ftype=="Industry":
    n = st.sidebar.selectbox("Number of Industries", options=[6,25],help="Which industry level to use")


metric_options = ['Return','BE/ME','Firm Count','Size']
if ftype in ["Portfolio","Industry"]:
    if ftype=="Portfolio":
        sort_options = ['Size','ME','BE','OP','INV']
        sorts = st.sidebar.selectbox("Sorts", options=sort_options)
    metric = st.sidebar.selectbox("Metric", options=metric_options)
else:
    metric = st.sidebar.selectbox("Metric", options=metric_options, disabled=True)

if ftype=='Industry':
    freq = st.sidebar.selectbox("Frequency",options=['Daily','Monthly'],index=1,disabled=True)
else:
    freq = st.sidebar.selectbox("Frequency",options=['Daily','Monthly'],index=1)    

if ftype == "Factor":    
    if n==3:
        item = f'F-F_Research_Data_Factors'
        # https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Research_Data_Factors_CSV.zip
    else:
        item = f'F-F_Research_Data_5_Factors_2x3'
    if freq=='Daily':
        item += '_daily'    
else:
    if n==6:
        item = f'{n}_Portfolios_2x3'
    elif n==25:
        item = f'{n}_Portfolios_5x5'
    elif n==100:
        item = f'{n}_Portfolios_10x10'        


@st.cache_data
def fetch_data(item):
    # create new data object, note that will always clear the .df, ie it won't be set upon instantiation
    dataobj = FFData(item)
    dataobj.read()
    return dataobj

dataobj = fetch_data(item)    
# dataobj.read()
df = dataobj.df
try:
    df['DateTime'] = pd.to_datetime(df['Date'],format='%Y%m%d')
except:
    try:
        df['DateTime'] = pd.to_datetime(df['Date'],format='%Y%m')
    except:
        # print(df['Date'])    
        pass
df.index=df['DateTime'].dt.date
df = df[[c for c in df.columns if c not in ['Date','DateTime']]]
df = df[df>-99]

def timeseries_plot(df,logy=False, width=plot_configs['width'], height=plot_configs['height']):
    if logy:
        fig = px.line(df, x=df.index, y=df.columns, title='Equity Risk Premia > Fama and French Five Factor Model',log_y=True,height=height)
    else:
        fig = px.line(df, x=df.index, y=df.columns, title='Equity Risk Premia > Fama and French Five Factor Model', height=height)
    fig.update_layout(margin=dict(l=20, r=20, t=40, b=20))
    return fig

_timseries_plot_container = st.empty()
_cummulative_timseries_plot_container = st.empty()

tab1, tab2, tab3 = st.tabs(['index','table','returns'])
with tab1:
    st.plotly_chart(timeseries_plot((1+df/100).cumprod(),True), use_container_width=True)
with tab2:
    # pd.options.display.float_format = "{:,.2f}".format
    st.dataframe(df.round(2), use_container_width=True, height=plot_configs['height'])
with tab3:    
    st.plotly_chart(timeseries_plot(df), use_container_width=True)
