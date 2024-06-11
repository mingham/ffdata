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

def map_sort_name_to_ff_item(s):
    if s == "Size":
        return "ME"
    if s == "Book Value":
        return "BE"
    if s == "Operating Profit":
        return "OP"
    if s == "Investment":
        return "INV" 

def parse_portfolio_file(filename):
    portfolio_items_nondaily = [  
        'Value Weight Returns -- Monthly',
        'Equal Weight Returns -- Monthly',
        'Value Weight Returns -- Annual from January to December',
        'Equal Weight Returns -- Annual from January to December',
        'Number of Firms in Portfolios',
        'Average Firm Size',
        'Sum of BE / Sum of ME',
        'Value Weight Average of BE / ME'
    ]
    skiprows = []
    with open(filename) as file:
        indf=False
        for line in file:
            for line_item in portfolio_items_nondaily:
                if line_item.lower() in line.lower():
                    data_rows=[]
                    indf=True
                else:
                    if indf:
                        if "," in line:
                            data_rows.append(line.split(','))        

            print(line.rstrip())    




def get_ff_file_name(freq,_type,n,sorts):

    if _type=="Factor":
        item = 'F-F_Research_Data'
        if n==5:
            item+='_5'
        item += '_Factors' 
        if n==5:
            item+='_2x3'
        if freq == 'Daily':
            item+='_daily'
        parse_type = 'factor'    
    elif _type=="Portfolio":
        if n==1:
            item = 'Portfolios_Formed_on'
            if sorts == ['Book to Market']:
                item += '_BE-ME'
            elif sorts == ['Size']:
                item += '_ME'    
            elif sorts == ['Book Value']:
                item += '_BE'    
            elif sorts == ['Operating Profit']:
                item += '_OP'    
            elif sorts == ['Investment']:
                item += '_INV'   
            else:
                # these only have monthly frequencies
                if sorts == ['Earnings Yield']:
                    item += "_E-P"
                elif sorts == ['Cashflow Yield']:
                    item += "_CF-P"    
                elif sorts == ['Dividend Yield']:
                    item += "_D-P"    
                elif sorts == ['Accruals']:
                    item += "_AC"     
                elif sorts == ['Market Beta']:
                    item += "_BETA"
                elif sorts == ['Net Share Issues']:
                    item += "_NI"                                   
                elif sorts == ['Daily Vol']:
                    item += "_VAR"       
                elif sorts == ['Residual Daily Vol']:
                    item += "_RESVAR"         
                freq = 'Monthly'    
            if freq == 'Daily':
                item+='_daily'
            parse_type = 'univariate'
        else:
            if n in [6,25,100]:
                item = f'{n}_Portfolios'
                if "Size" in sorts and "Book to Market" in sorts:
                    if n==6:
                        item += '_2x3'
                    elif n==25:
                        item += '_5x5'    
                    elif n==100:
                        item += '_10x10'    
                else:
                    if "Size" in sorts:
                        other_sort = [c for c in sorts if c!="Size"]
                        if len(other_sort)>0:
                            other_sort =  other_sort[0]
                            item+=f'_ME_{map_sort_name_to_ff_item(other_sort)}'                        
                        else:
                            item+=f'_ME'                            
                    else:
                        if "Book to Market" in sorts:
                            other_sort = [c for c in sorts if c!="Book to Market"][0]
                            if len(other_sort)>0:
                                other_sort =  other_sort[0]
                                item+=f'_BE-ME_{map_sort_name_to_ff_item(other_sort)}'
                            else:
                                item+=f'_BE-ME'  
                        else:
                            if "Operating Profit" in sorts:
                                other_sort = [c for c in sorts if c!="Operating Profit"][0]
                                if len(other_sort)>0:
                                    if other_sort == "Investment":
                                        item+=f'_OP_INV'
                                else:
                                    item+='_OP'        
                    if freq == 'Daily':
                        item+='_daily'
            else:
                # three way sort  
                if "Size" in sorts and "Book to Market" in sorts and "Operating Profit" in sorts:
                    item+='_ME_BEME_OP'
                if "Size" in sorts and "Book to Market" in sorts and "Investment" in sorts:
                    item+='_ME_BEME_INV'
                if "Size" in sorts and "Operating Profit" in sorts and "Investment" in sorts:
                    item+='_ME_OP_INV'    
                if n==32:
                    item+='_2x4x4'    
    elif _type=="Industry":
        pass
    return item
        


def drop_table(freq="Monthly"):
    conn = duckdb.connect(database=DUCKDB_DBNAME, read_only=False)
    cursor = conn.cursor()
    cursor.execute(f'DROP TABLE {freq}_Data')
    conn.commit()
    conn.close()

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
        df = pd.read_csv(abspath,  skiprows=self._get_skiprows())
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

    def read(self, force=False):        
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

class FFDataDuck(FFData):            
    def __init__(self, freq="Monthly", _type="Factor", n=5, metric="Returns", weighting="Value Weighted", sorts="") -> None:        
        self.db = DUCKDB_DBNAME
        self.freq=freq
        self._type = _type
        self.n = n
        self.metric = metric
        self.sorts = sorts
        self.weighting = weighting
        self.item = get_ff_file_name(freq,_type,n,sorts)
    # Function to parse data and load it into DuckDB
    def _get_duckdb_sample(self):
        conn = duckdb.connect(database=self.db, read_only=False)
        cursor = conn.cursor()
        cursor.execute(f'SELECT * FROM {self.freq}_Data')
        data = cursor.df()
        conn.close()
        return data

    def _load_csv_into_duckdb(self):
        # item=self.item
        fn=f'{self.item}.csv'
         
        abspath = os.path.join(self.data_dir,self.item,'.csv',fn)

        if 'portfolio' in self.item.lower():
            list_of_dfs = parse_portfolio_file(abspath)
        else:
            pass
            # list_of_dfs = extract_tables_from_csv(self.item)

        for df in list_of_dfs:
            df = pd.read_csv(abspath, delimiter=",", skiprows=self._get_skiprows())            
            df['_type'] = self._type
            df['n'] = self.n
            df['metric'] = self.metric
            df['sorts'] = self.sorts

            cols = ['Date'] + df.columns[1:].tolist()
            df.columns=cols        
            df2 = df.melt(id_vars=['Date', '_type', 'n', 'metric', 'sorts'])

            conn = duckdb.connect(database=self.db, read_only=False)
            cursor = conn.cursor()
            print(self.item)
            print(self._check_table_existence())
            if not self._check_table_existence():
                cursor.execute(f'CREATE TABLE {self.freq}_Data AS SELECT * FROM df2')
            else:                
                cursor.execute(f'INSERT INTO {self.freq}_Data SELECT * FROM df2')


            # conn.register('data', df)
            # conn.execute("CREATE TABLE {} AS SELECT * FROM data".format(self.item))
            conn.commit()
            conn.close()

    def _check_table_existence(self):
        try:
            conn = duckdb.connect(self.db)
            cursor = conn.cursor()
            cursor.execute(f'SELECT * FROM "{self.freq}_Data" limit 10')
            conn.commit()
            conn.close()
            return True
        except Exception as e:
                if "Table not found" in str(e):
                    return False
                else:
                    print("Error:", e)
                    return False
                
    def _get_csv_data_from_db(self):
        conn = duckdb.connect(self.db)
        cursor = conn.cursor()
        # don't filter on metric because different metrics are often in same csv file
        cursor.execute(f"""
            SELECT * 
            FROM {self.freq}_DATA
            WHERE  _type = '{self._type}'
            AND        n = {self.n}
            AND    sorts = '{self.sorts}'
        """)
        return cursor.df()     

    def read(self):
        # need to check for existing rows here.  If already there then just read the
        # relevant records

        conn = duckdb.connect(self.db)
        cursor = conn.cursor()

        df = self._get_csv_data_from_db()
        csv_data_already_exists = len(df.index)>0

        if not csv_data_already_exists:
            self._request_and_extract_zip_file(f'{self.base_url}/{self.item}_CSV.zip',self.item)
            print(self.item)
            self._load_csv_into_duckdb()
            print("Table", self.item, "created.")
            cursor.execute(f'SELECT * FROM "{self.item}"')
            data = cursor.df()
            conn.close()
            self.df = data
        else:
            conn.close()
            self.df = df    


def __format_date(t):
    pass


def extract_tables_from_csv(item):
    """
    a more genral table extractor.  It can handle annual returns, value weighted averages, etc
    """
    pass


if __name__=='__main__':
    # item='49_Industry_Portfolios'
    item = 'F-F_Research_Data_5_Factors_2x3'
    obj = FFDataDuck(item)
    obj.read()
    print(obj.df.head())

    # df = obj._get_duckdb_sample()
    # print(df)
    # drop_table()
    # df = obj._get_duckdb_sample()
    # print(df)
