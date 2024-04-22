from shutil import rmtree
import sqlite3
import pandas as pd
from pathlib import Path
import os
# import pandas_datareader.data as web
# from pandas_datareader.yahoo.headers import DEFAULT_HEADERS
import datetime
from requests_cache import CachedSession
from datetime import timedelta
import requests, zipfile, io

plot_configs = {
    'height':600,
    'width': 700
}

class RemoteData:
    def __init__(self) -> None:
        self.data_dir = os.path.join(Path(__file__).parent.parent.absolute(),'data')

    def read(self, item):
        pass

class FFData(RemoteData):
    def __init__(self, item) -> None:
        super().__init__()
        self.base_url = 'https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp'
        self.data_dir = os.path.join(self.data_dir,'FamaFrench')
        self.item = item
        self.parquet_exists = os.path.exists(os.path.join(self.data_dir,f'{item}.parquet'))
        self.df = pd.DataFrame()
        self.isread=False

    def _get_data_path(self):
    # print(os.path.join(Path(__file__).parent.parent.absolute(),'data','FamaFrench'))
        return os.path.join(Path(__file__).parent.parent.absolute(),'data','FamaFrench')

    def _request_and_extract_zip_file(self,url, savename):
        r = requests.get(url)
        z = zipfile.ZipFile(io.BytesIO(r.content))
        z.extractall(os.path.join(self._get_data_path(), savename, '.csv'))


    def _load_csv_to_parquet(self):
        item=self.item
        fn=f'{item}.csv'
        abspath = os.path.join(self._get_data_path(),item,'.csv',fn)

        # read_csv moans about the 4-th row in original file (this is the headers row)
        # df=pd.read_csv(abspath,delimiter=',',skiprows=3)    
        if item in ['49_Industry_Portfolios']:
            df=pd.read_csv(abspath,delimiter=',',skiprows=11)    
        else:
            df=pd.read_csv(abspath,delimiter=',',skiprows=3)                    
        columns = df.columns.to_list()
        df.columns = ['Date'] + columns[1:]

        cnt=0
        idx=-1
        for t in df['Date']:
            if idx==-1:
                if type(t)==str:                    
                    if 'Annual' in t:
                        idx=cnt
                    elif 'Average' in t:
                        idx=cnt    
                    elif 'Sum' in t:
                        idx=cnt    
                    else:
                        cnt+=1    
                else:
                    cnt+=1
        if idx>-1:
            df=df.iloc[:idx]
        print(df.dtypes)    
        dtypes={}
        for col in df.columns:
            if col=='Date':
                dtypes[col]='Int64'
            else:
                dtypes[col]='Float64'    
        df=df.astype(dtypes)
        # df=df.convert_dtypes()

        abspath_parquet = os.path.join(self._get_data_path(),fn.replace('.csv','.parquet'))
        df.to_parquet(abspath_parquet)
        rmtree(os.path.join(self._get_data_path(),item))     

    def read(self, force=False):
        if not self.parquet_exists or force:
            print('EXTRACTING',self.item)
            # request_and_extract_zip_file(f'{self.base_url}/{item}_CSV.zip',item)
            # load_csv_to_parquet(item)
            self._request_and_extract_zip_file(f'{self.base_url}/{self.item}_CSV.zip',self.item)
            self._load_csv_to_parquet()            
        self.df = pd.read_parquet(os.path.join(self.data_dir,f'{self.item}.parquet'))
        if self.df.shape[0]>0:
            self.isread = True

def __format_date(t):
    pass

# def request_session_data(url):
#     session = CachedSession('ff_cache', expire_after=timedelta(days=3))
#     return session.get(url)

if __name__=='__main__':
    # item='49_Industry_Portfolios'
    # item = 'F-F_Research_Data_5_Factors_2x3'
    item = 'F-F_Research_Data_5_Factors_2x3_daily'
    obj = FFData(item)
    obj.read()
