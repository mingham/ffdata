import plotly.express as px
import pandas as pd
from pathlib import Path
import os
import sys
import streamlit as st

sys.path.append(os.path.join(Path(__file__).parent.absolute(),'data'))
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

item = st.sidebar.selectbox("item", options=['F-F_Research_Data_5_Factors_2x3_daily','F-F_Research_Data_5_Factors_2x3','49_Industry_Portfolios'])

@st.cache_data
def fetch_data(item):
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
        fig = px.line(df, x=df.index, y=df.columns, title='Equity RP',log_y=True,height=height)
    else:
        fig = px.line(df, x=df.index, y=df.columns, title='Equity RP', height=height)
    fig.update_layout(margin=dict(l=20, r=20, t=20, b=20))
    return fig

_timseries_plot_container = st.empty()
_cummulative_timseries_plot_container = st.empty()

tab1, tab2, tab3 = st.tabs(['index','table','returns'])
with tab1:
    st.plotly_chart(timeseries_plot((1+df/100).cumprod(),True), use_container_width=True)
with tab2:
    st.dataframe(df, use_container_width=True, height=plot_configs['height'])
with tab3:    
    st.plotly_chart(timeseries_plot(df), use_container_width=True)



# tab1 = st.empty()
# tab2 = st.empty()
# tab3 = st.empty()
# with tab2:
#     st.dataframe(df, use_container_width=True)
# with tab3:    
#     st.plotly_chart(
#         timeseries_plot(df)
#     )
# with tab1:    
#     st.plotly_chart(
#         timeseries_plot((1+df/100).cumprod(),True)
#     )
