import duckdb
from shutil import rmtree
import sqlite3
import pandas as pd
from pathlib import Path
import os
import datetime
from requests_cache import CachedSession
from datetime import timedelta
import requests, zipfile, io
import sys

plot_configs = {
    'height':600,
    'width': 700
}

DUCKDB_DBNAME = 'ffdata.duckdb'

# duckdb
def download_data(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.content.decode('utf-8')
    else:
        print("Failed to download data from", url)
        return None

# Function to parse data and load it into DuckDB
def load_data_into_duckdb(data, table_name):
    df = pd.read_csv(data, delim_whitespace=True, skiprows=11)
    conn = duckdb.connect(database=':memory:', read_only=False)
    conn.register('data', df)
    conn.execute("CREATE TABLE {} AS SELECT * FROM data".format(table_name))
    print("Data loaded into DuckDB table:", table_name)

def main():
    # List of data files to download
    data_files = [
        "http://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Research_Data_Factors_ASCII.txt",
        "http://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Research_Data_Factors_weekly_ASCII.txt",
        "http://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Research_Data_Factors_daily_ASCII.txt",
        "http://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Research_Data_Factors_daily_CSV.zip",
        "http://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Research_Data_5_Factors_2x3_ASCII.txt",
        "http://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Research_Data_5_Factors_2x3_daily_ASCII.txt",
        "http://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Momentum_Factor_daily_ASCII.txt",
        "http://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Momentum_Factor_CSV.zip"
    ]

    # Download and load data into DuckDB
    for url in data_files:
        print("Downloading data from", url)
        data = download_data(url)
        if data is not None:
            filename = url.split("/")[-1].split(".")[0]
            if filename.endswith("_ASCII"):
                table_name = filename.replace("_ASCII", "")
            elif filename.endswith("_CSV"):
                table_name = filename.replace("_CSV", "")
            else:
                table_name = filename
            load_data_into_duckdb(data, table_name)

# duckdb
class RemoteData:
    def __init__(self) -> None:
        # self.data_dir = os.path.join(Path(__file__).parent.parent.absolute(),'data')
        self.data_dir = os.path.join(Path(__file__).parent.parent.parent.absolute(),'db','parquet')        

    def read(self, item):
        pass

class FFData(RemoteData):
    def __init__(self, item) -> None:
        super().__init__()
        self.base_url = 'https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp'
        # self.data_dir = os.path.join(self.data_dir,'FamaFrench')
        self.item = item
        self.parquet_exists = os.path.exists(os.path.join(self.data_dir,f'{item}.parquet'))
        self.df = pd.DataFrame()
        self.isread=False

    def _request_cache(self, url, days_expiry=3):
        """
        A nice caching mechanism built on top of request library.  You can
        set a number of days to expiry param (default=3)
        """
        session = CachedSession('ff_cache', expire_after=timedelta(days=days_expiry))
        return session.get(url)

    def _request_and_extract_zip_file(self,url, savename):
        # r = requests.get(url)
        r = self._request_cache(url)
        z = zipfile.ZipFile(io.BytesIO(r.content))
        z.extractall(os.path.join(self.data_dir, savename, '.csv'))

    def _get_skiprows(self):
        if self.item in ['49_Industry_Portfolios']:
            return 11
        else:
            return 3

    # Function to parse data and load it into DuckDB
    def _load_csv_into_duckdb(self):
        # item=self.item
        fn=f'{self.item}.csv'
        abspath = os.path.join(self.data_dir,self.item,'.csv',fn)
        df = pd.read_csv(abspath, delim_whitespace=True, skiprows=self._get_skiprows())
        conn = duckdb.connect(database=DUCKDB_DBNAME, read_only=False)
        conn.register('data', df)
        conn.execute("CREATE TABLE {} AS SELECT * FROM data".format(self.item))
        conn.commit()
        conn.close()

    def _load_csv_into_parquet(self):
        item=self.item
        fn=f'{item}.csv'
        abspath = os.path.join(self.data_dir,item,'.csv',fn)

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

        abspath_parquet = os.path.join(self.data_dir,fn.replace('.csv','.parquet'))
        df.to_parquet(abspath_parquet)
        self.df = df
        self.isread = True
        rmtree(os.path.join(self.data_dir,item))     

    def read(self, force=False, duckdb=False):
        if duckdb:
            conn = duckdb.connect(DUCKDB_DBNAME)
            cursor = conn.cursor()
            # Check if the table exists
            cursor.execute("SELECT COUNT(*) FROM duckdb_catalog.tables WHERE name = ?", (self.item,))
            result = cursor.fetchone()
            table_exists = result[0] > 0

            if table_exists:
                self._load_csv_into_duckdb()
                # Table exists, read data from it
                cursor.execute("SELECT * FROM {}".format(self.item))
                data = cursor.fetchall()
                conn.close()
                return data
            else:
                conn.close()
                self._load_csv_into_duckdb()
                print("Table", table_name, "created.")
                return None
        else:            
            if not self.parquet_exists or force:
                print('EXTRACTING',self.item)
                # request_and_extract_zip_file(f'{self.base_url}/{item}_CSV.zip',item)
                # load_csv_to_parquet(item)
                self._request_and_extract_zip_file(f'{self.base_url}/{self.item}_CSV.zip',self.item)
                self._load_csv_into_parquet()   
                # _load_csv_into_parquet automatically adds .df field at end
            else:
                self.df = pd.read_parquet(os.path.join(self.data_dir,f'{self.item}.parquet'))
                self.isread = True
            if self.df.shape[0]>0:
                self.isread = True

def __format_date(t):
    pass



if __name__=='__main__':
    # item='49_Industry_Portfolios'
    item = 'F-F_Research_Data_5_Factors_2x3'
    obj = FFData(item)
    obj._load_csv_into_duckdb()
    obj.read()
