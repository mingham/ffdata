import duckdb
import plotly.express as px
import pandas as pd
from pathlib import Path
import os
import sys
import streamlit as st

# sys.path.append(os.path.join(Path(__file__).parent.parent.parent.absolute(),'db','data'))
from data import FFData, plot_configs

st.set_page_config(
    page_title="Returns Report",
    layout="wide"
)
st.markdown("""
        <style>
               .block-container {
                    padding-top: 2rem;
                    padding-bottom: 0rem;
                    padding-left: 1rem;
                    padding-right: 1rem;
                }
        </style>
        """, unsafe_allow_html=True)

# DUCKDB_DBNAME = '../../../../ffdata.duckdb'
DUCKDB_DBNAME = '~/ffdata/ffdata.duckdb'

def timeseries_plot(df,logy=False, width=plot_configs['width'], height=plot_configs['height']):
    if logy:
        fig = px.line(df, x=df.index, y=df.columns, title='Equity Portfolio Returns',log_y=True,height=height)
    else:
        fig = px.line(df, x=df.index, y=df.columns, title='Equity Portfolio Returns', height=height)
    fig.update_layout(margin=dict(l=20, r=20, t=40, b=20))
    return fig

# @st.cache_data
def read_data(db=DUCKDB_DBNAME, start=0,end=0,metric='Weighted Returns', region='US',n=6,sort_1='',sort_2='',sort_3=''):
    # print('n is',n)
    assert n in [6,25,32,36,100], "n can only be one of; 6, 25, 32, 36, 100"
    con = duckdb.connect(database=db,read_only=True)
    sql_str = f'''
        SELECT * 
        FROM Monthly_Portfolio_Data
        WHERE region = '{region}'
        AND metric = '{metric}'
        AND n = {n}
    '''
    if start>0:
        sql_str+= f' AND start>={start}'
    if end>0:
        sql_str+= f' AND end<={end}'
    if sort_1:
        sql_str+= f" AND sort_1='{sort_1}'"
    if sort_2:
        sql_str+= f" AND sort_2='{sort_2}'"
    if sort_3:
        sql_str+= f" AND sort_3='{sort_3}'"
    else:
        if n==32:
            sort_3='OP'
            sql_str+= f" AND sort_3='{sort_3}'"        

    print(sql_str)
    result = con.execute(sql_str)
    rows = result.fetchall()
    cols = [elt[0] for elt in result.description]
    df = pd.DataFrame(rows, columns=cols)
    con.close()

    cols=[]
    for c in df.columns:
        if c != 'value':
            cols.append(c)
        else:
            cols.append('data')    
    # df['_id'] = df['metric'] + ':' + df['region'] + ':' + str(df['n']) + ':' + df['sort_1'] + ':' + df['sort_2'] + ':' + df['sort_3']
    df['_id'] = df['metric'] + ':' + df['region'] + ':' + df['n'].astype(str) + ':' + df['sort_1'] + ':' + df['sort_2'] + ':' + df['sort_3'] + ':' + df['variable']
    df = df.pivot(index='date',columns=['variable'],values='value')
    df['date'] = pd.to_datetime(df.index.astype(str), format='%Y%m')
    df['date'] = df['date'].dt.date
    df = df.set_index('date')

    return df


# st.markdown("""
#             # View the Data as a Table
#             """)


options_dict = {
    'start': '0',
    'end': '0',
    'metric': ['Weighted Returns','Equal-Weighted Returns'],
    'region': ['Asia_Pacific_ex_Japan','Developed','Developed_ex_US','Emerging','Europe','Japan','North_America','US'],
    'n': ['6','25','32','100'],
    'sort_1': ['ME','BE-ME','OP','INV'],
    'sort_2': ['BE-ME','OP','INV'],
    'sort_3': ['OP','INV'],    
}

col1, col2 = st.sidebar.columns(2)
with col1:
    start = st.sidebar.text_input("Start", value='0',placeholder='0')
    start=int(start)
with col2:
    end = st.sidebar.text_input("End", value='0',placeholder='0')
    end = int(end)
metric = st.sidebar.selectbox("Return",options=['Weighted Returns','Equal-Weighted Returns'])
region = st.sidebar.selectbox("Region",options=['Asia_Pacific_ex_Japan','Developed','Developed_ex_US','Emerging','Europe','Japan','North_America','US'])
n = int(st.sidebar.selectbox("Number of Portfolios",options=['6','25','32','100']))
sort_1 = st.sidebar.selectbox("First Sort",options=['ME','BE-ME','OP','INV'])
if n in [6,25,32,100]:
    sort_2 = st.sidebar.selectbox("Second Sort",options=['BE-ME','OP','INV'])
else:
    sort_2 = st.sidebar.selectbox("Second Sort",options=['','BE-ME','OP','INV'])
if n==32:
    sort_3 = st.sidebar.selectbox("Third Sort",options=['OP','INV'])
else:
    sort_3 = st.sidebar.selectbox("Third Sort",options=[''],disabled=True)

df = read_data(start=start, end=end, metric=metric, region=region, n=n, sort_1=sort_1, sort_2=sort_2, sort_3=sort_3)
# read_data(db=DUCKDB_DBNAME, start=0,end=0,metric='Weighted Returns', region='US',n=6)



_timseries_plot_container = st.empty()
_cummulative_timseries_plot_container = st.empty()

tab1, tab2, tab3 = st.tabs(['index','table','returns'])
with tab1:
    st.plotly_chart(timeseries_plot((1+df/100).cumprod(),True), use_container_width=True)
with tab2:
    st.dataframe(df.round(2), use_container_width=True, height=plot_configs['height'])
with tab3:
    st.plotly_chart(timeseries_plot(df), use_container_width=True)

