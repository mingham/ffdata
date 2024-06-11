# this data file is for global stuff and not part of the app
# eg you can build a duckdb from all files on website
# note that the duckdb file resides outside of app
import timeit
import pandas as pd
import re
import duckdb
import os
import requests
from bs4 import BeautifulSoup, SoupStrainer
from requests_cache import CachedSession
from datetime import timedelta
import requests, zipfile, io
from pathlib import Path

DUCKDB_DBNAME = 'ffdata.duckdb'
BASE_URL = 'https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/'
CSV_DIR = os.path.join(Path(__file__).parent.parent.parent.absolute(),'db','raw_csvs')


def get_full_path_name(filename):
    """
    Returns full path name
    """
    p=Path(filename)
    name = p.name
    if name[-4:].lower() == '.csv':
        name = name[:-4]
    fullpath = os.path.join(CSV_DIR, name, '.csv', f'{name}.CSV')
    return fullpath



def capture_between_on_and_end(text):
    pattern = r'_on_(.*)$'
    match = re.search(pattern, text)
    if match:
        return match.group(1)
    else:
        return None

def determine_table_format(filename):
    filename = filename.replace('.CSV','.csv')
    p=Path(filename)
    name = p.name.replace('.csv','')
    # US only - original and more detailed - derived from Compustat database
    core_fields = [
        'Weighted Returns',
        'Equal-Weighted Returns',            
        'Weighted Returns - Annual',
        'Equal-Weighted Returns - Annual',
        'Number of Firms in Portfolios',
        'Average Market Cap',
    ]
    # pattern = r"^\d+_Portfolios_[A-Z]+_[A-Z]"
    patterns = [
        r"^\d+_Portfolios_ME_INV",
        r"^\d+_Portfolios_ME_OP",
        r"^\d+_Portfolios_BEME_INV",
        r"^\d+_Portfolios_BEME_OP",
        r"^\d+_Portfolios_OP_INV",
    ]
    extra_fields = [
        'Value Weighted Average of BE/ME',
        'Value Weighted Average of BE',
        'Value Weighted Average of OP',
        'Value Weighted Average of INV',
    ]
    matches = [bool(re.match(pattern, name)) for pattern in patterns]
    if any(matches):
        return core_fields + extra_fields

    patterns = [
        r"^\d+_Portfolios_ME_AC",
        r"^\d+_Portfolios_ME_NI",
    ]
    extra_fields = [
        'Value Weighted Average of BE/ME',
        'Value Weighted Average of BE',
        'Value Weighted Average of NI',
        'Value Weighted Average of AC',
        'Value Weighted Average of OP',
        'Value Weighted Average of INV',
    ]
    matches = [bool(re.match(pattern, name)) for pattern in patterns]
    if any(matches):
        return core_fields + extra_fields

    patterns = [
        r"^\d+_Portfolios_ME_Prior_.*$",
    ]
    extra_fields = [
        'Equal Weighted Prior Returns',
        'Value Weighted Prior Returns',        
    ]
    matches = [bool(re.match(pattern, name)) for pattern in patterns]
    if any(matches):
        return core_fields + extra_fields

    patterns = [
        r"^\d+_Portfolios_ME_VAR_.*$",
    ]
    extra_fields = [
        'Value Weighted Past Returns Variance',        
        'Value Weighted Average of BE/ME',
        'Value Weighted Average of BE',
        'Value Weighted Average of OP',
        'Value Weighted Average of INV',
    ]
    matches = [bool(re.match(pattern, name)) for pattern in patterns]
    if any(matches):
        return core_fields + extra_fields

    patterns = [
        r"^\d+_Portfolios_ME_RESVAR_.*$",
    ]
    extra_fields = [
        'Value Weighted Past Residual Returns Variance',   
        'Value Weighted Average of BE/ME',
        'Value Weighted Average of BE',
        'Value Weighted Average of OP',
        'Value Weighted Average of INV',
    ]
    matches = [bool(re.match(pattern, name)) for pattern in patterns]
    if any(matches):
        return core_fields + extra_fields

    pattern = r"^\d+_Portfolios"
    if bool(re.match(pattern, name)):
        extra_fields = [
            'Value Weighted Average of BE/ME',
            'Value Weighted Average of BE',
            'Value Weighted Average of OP',
            'Value Weighted Average of INV',
        ]
        return core_fields + extra_fields

    pattern = r"^\d+_Industry_Portfolios"
    if bool(re.match(pattern, name)):
        extra_fields = [
            'Aggregate BE/ME - Annual',
            'Value Weighted Average of BE/ME - Annual',
        ]
        return core_fields + extra_fields



    # Regional portfolios and Inustry - derived from Bloomberg database
    patterns = [
        r"^[A-Za-z_]+_\d+_Portfolios.*$", 
    ]
    matches = [bool(re.match(pattern, name)) for pattern in patterns]
    if any(matches):
        return core_fields
    
    pattern = r"^Portfolios_Formed_on_.*$"
    if bool(re.match(pattern, name)):
        metric = capture_between_on_and_end(name)
        print('metric is',metric)
        if '-' in name:            
            if '_' not in metric:
                extra_fields = [
                    f'Aggregate {metric} - Annual',
                    f'Weighted {metric} - Annual'
                ]
                return core_fields + extra_fields
            else:
                return core_fields
        else:
            if metric == 'ME':
                return core_fields
            elif metric == 'INV':
                return core_fields + [f'Weighted Average of log(INV) - Annual']
            if '_' not in metric:
                return core_fields + [f'Weighted Average of {metric} - Annual']            
            else:
                return

def parse_core_fields_from_csv_file(filename):
    """
    this just gets the top 6 tables form the given filename:

        - Value Weighted Returns (Monthly)
        - Equal Weighted Returns (Monthly)
        - Value Weighted Returns (Annual)
        - Equal Weighted Returns (Annual)
        - Number of Firms (Monthly)
        - Average Firm Size (Monthly)

    """
    table_names = [
        'Weighted Returns',
        'Equal-Weighted Returns',            
        'Weighted Returns - Annual',
        'Equal-Weighted Returns - Annual',
        'Number of Firms in Portfolios',
        'Average Market Cap',
    ]
    table_names = [
        'Weighted Returns',
        'Equal-Weighted Returns',            
    ]
    fullname = get_full_path_name(filename)
    if '_weekly' in fullname.lower():
        pass
    elif '_daily' in fullname.lower():
        pass
    else:
        with open(fullname, encoding='ISO-8859-1') as file:
            result=[]
            indf=False
            data_rows=[]
            cnt=-1
            all_lines = file.readlines()
            for i,line in enumerate(all_lines):
                if cnt<len(table_names):
                    line=line.strip()
                    if len(line)>0:
                        if "," == line[0]:
                            cnt+=1
                            # try:
                            #     print('Parsing ', table_names[cnt])                    
                            # except:
                            #     print(line)    
                            # header row of a table
                            headers = ['Date'] + line.split(",")[1:]                    
                            data_rows = []
                            indf = True
                        elif "," in line:
                            row = line.split(',')
                            try:
                                row = [int(row[0])] + [float(x) for x in row[1:]]
                                data_rows.append(row)

                                # check we're not on last line.  If we are then write out dataframe
                                if i==len(all_lines)-1:
                                    obj = {
                                        'name': table_names[cnt],
                                        'df': pd.DataFrame(columns=headers, data=data_rows)
                                    }
                                    result.append(obj)
                            except:
                                pass        
                        else:                    
                            if indf:                        
                                obj = {
                                    'name': table_names[cnt],
                                    'df': pd.DataFrame(columns=headers, data=data_rows)
                                }                            
                                result.append(obj)
                                indf=False
                                data_rows = []
                else:
                    break                
            return result                 
    return

def parse_csv_file(filename):
    fullname = get_full_path_name(filename)
    result=[]
    table_names = determine_table_format(filename)
    print(table_names)
    with open(fullname) as file:
        indf=False
        data_rows=[]
        cnt=-1
        all_lines = file.readlines()
        for i,line in enumerate(all_lines):
            line=line.strip()
            if len(line)>0:
                if "," == line[0]:
                    cnt+=1
                    try:
                        print('Parsing ', table_names[cnt])                    
                    except:
                        print(line)    
                    # header row of a table
                    headers = ['Date'] + line.split(",")[1:]                    
                    data_rows = []
                    indf = True
                elif "," in line:
                    row = line.split(',')
                    # print('LEN OF ROW IS:')
                    # print(len(row))
                    data_rows.append(row)

                    # check we're not on last line.  If we are then write out dataframe
                    if i==len(all_lines)-1:
                        obj = {
                            'name': table_names[cnt],
                            'df': pd.DataFrame(columns=headers, data=data_rows)
                        }
                        result.append(obj)
                else:                    
                    if indf:                        
                        obj = {
                            'name': table_names[cnt],
                            'df': pd.DataFrame(columns=headers, data=data_rows)
                        }
                        print(obj['df'].head())
                        result.append(obj)
                        indf=False
                        data_rows = []
        return result                 
    


def get_all_ftp_csv_links(base_url=BASE_URL):    
    main_page_url = base_url + 'data_library.html'
    response = requests.get(main_page_url)
    cnt=0
    result=[]
    for link in BeautifulSoup(response.text, parse_only=SoupStrainer('a'), features='html.parser'):
        if link.has_attr('href'):
            if '_CSV.zip' in link['href']:
                cnt+=1
                result.append(link['href'])    
    print('Total Number of Files = ', cnt)
    return result

def request_cache(url, days_expiry=3):
        """
        A nice caching mechanism built on top of request library.  You can
        set a number of days to expiry param (default=3)
        """
        session = CachedSession('ff_cache', expire_after=timedelta(days=days_expiry))
        return session.get(url)

def request_and_extract_zip_file(ftp_link):
    """
    Uses requests library to get files from K. French website and then extracts
    them to local.
    """
    # r = requests.get(url)
    r = request_cache(os.path.join(BASE_URL,ftp_link))
    z = zipfile.ZipFile(io.BytesIO(r.content))
    savename = ftp_link.replace('_CSV.zip','').replace('ftp/','')
    z.extractall(os.path.join(CSV_DIR, savename, '.csv'))

def extract_all_csvs():
    links = get_all_ftp_csv_links()
    for link in links:
         print('extracting',link)
         request_and_extract_zip_file(link)         

def get_all_extracted_filenames(root_dir=CSV_DIR):
    # Iterate over each subdirectory in the root directory
    result = []
    for subdir, dirs, files in os.walk(root_dir):
        # print(f"Subdirectory: {subdir}")
        # Iterate over each file in the current subdirectory
        if subdir != 'non_monthly':
            for file in files:
                # print(f"    {file}")
                result.append(file)
    return result            


def load_returns_into_duckdb():
    """
    This takes dataframes from the output of parse_csv_file, extracts
    the returns data and then loads into duckdb
    """
#     core_fields = [
#     'Weighted Returns',
#     'Equal-Weighted Returns',            
#     'Weighted Returns - Annual',
#     'Equal-Weighted Returns - Annual',
#     'Number of Firms in Portfolios',
#     'Average Market Cap',
# ]

    for filename in get_all_extracted_filenames():
        list_of_dfs = parse_csv_file(filename)
        # get portfolio type, n and sorts from filename
        # or get the above info within parse_csv_file?
        pass

# parse_core_fields_from_csv_file

def load_all_data_into_duck_db(limit=None):
    """
    iterates over all extracted file names, computes dataframes based
    on the data in the files and then writes these dataframes to duckdb
    """
    # for filenames in 
    all_extracted_filenames = get_all_extracted_filenames()
    if limit:
        all_extracted_filenames = all_extracted_filenames[:min(limit,len(all_extracted_filenames))]
    for filename in all_extracted_filenames:
        inputs = get_portfolio_inputs_from_filename(filename)
        if inputs != None:
            print(inputs)
            data_objects = parse_core_fields_from_csv_file(filename)
            print(data_objects)
            if data_objects:
    # (region, n, sort_1, sort_1_n, sort_2, sort_2_n, sort_3, sort_3_n)
                for obj in data_objects:
                    df=obj['df']
                    df['region'] = str(inputs[0])
                    df['n'] = int(inputs[1])
                    df['sort_1'] = str(inputs[2])
                    if len(inputs)>3:
                        df['sort_1_n'] = int(inputs[3])
                    else:
                        df['sort_1_n'] = int(inputs[1])   
                    if len(inputs)>4:
                        df['sort_2'] = str(inputs[4])
                    else:
                        df['sort_2'] = ''   
                    if len(inputs)>5:
                        df['sort_2_n'] = int(inputs[5])
                    else:
                        df['sort_2_n'] = 0                            
                    if len(inputs)>6:    
                        df['sort_3'] = str(inputs[6])
                    else:
                        df['sort_3'] = ''    
                    if len(inputs)>7:    
                        df['sort_3_n'] = int(inputs[7])
                    else:
                        df['sort_3_n'] = 0    
                    df['metric'] = str(obj['name'].replace(' -- Annual',''))
                    # df = df.set_index('Date')
                    # print(df.head())
                    print(df.dtypes)
                    # df['date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d')
                    df['date'] = df['Date']
                    molten = pd.melt(df, id_vars=['metric','region','n','sort_1','sort_1_n','sort_2','sort_2_n','sort_3','sort_3_n','date'], value_vars=[c for c in df.columns if c not in ['date','Date']])
                    print('Committing to DuckDB',filename,obj['name'])

                    db='Monthly_Portfolio'
                    if 'Annual' in obj['name']:
                        db='Annual_Portfolio'
                    print('db is',db)
                    conn = duckdb.connect(database=DUCKDB_DBNAME, read_only=False)
                    cursor = conn.cursor()

                    try:
                        
                        cursor.execute(f'SELECT * FROM "{db}_Data" limit 10')
                        conn.commit()
                        print('inserting ' + obj['name'])
                        cursor.execute(f'INSERT INTO {db}_Data SELECT * FROM molten')
                        print('success in inserting ' + obj['name'])
                    except:
                        # Get rows containing at least one space
                        print('creating')
                        for col in df.columns:
                            rows_with_space = df[df[col]=='']
                            print(rows_with_space)
                        cursor.execute(f'CREATE TABLE {db}_Data AS SELECT * FROM molten')
                        print('created')
                    conn.commit()    
                    conn.close



                # load dataframe into duckdb


                # print('Data Object is:', obj['name'])
                # print(obj['df'].head())




    # pass

def get_portfolio_inputs_from_filename(filename):
    """
    get portfolio region, n (number of portfolios) and factors (that were used to form the portfolios) from filename    
    """
    # sanitise filename
    name = filename.replace('.CSV','').replace('.csv','')

    # don't want price returns
    if '_Wout_Div' in filename:
        return
    
    # helper function to set sorts
    sort_1 = ''
    sort_1_n = 0
    sort_2 = ''
    sort_2_n = 0
    sort_3 = ''
    sort_3_n = 0
    def reset_splits():
        sort_1 = ''
        sort_1_n = 0
        sort_2 = ''
        sort_2_n = 0
        sort_3 = ''
        sort_3_n = 0

    # now go through portfolios patterns in same order as on KFrenhc website

    #  UNIVARIATE SORTS
    # Univariate sorts on many factors, including accruals, net issuance as well as more standard factors
    pattern = r"^Portfolios_Formed_on_([A-Z-]+)$"
    match = re.search(pattern, name)
    if match:
        print('matched US one-way (non-monentum) sorts')
        region='US'
        n=10
        reset_splits()
        sort_1 = match.group(1)
        sort_1_n = 10
        return (region, n, sort_1, sort_1_n, sort_2, sort_2_n, sort_3, sort_3_n)

    

    # momentum univariate sorts
    pattern = r"^(\d+)_Portfolios_(Prior_.*)$"
    match = re.search(pattern, name)
    if match:
        print('matched US one-way momentum sorts')
        region='US'
        n=10
        reset_splits()
        sort_1 = match.group(1)
        sort_1_n = 10
        return (region, n, sort_1, sort_1_n, sort_2, sort_2_n, sort_3, sort_3_n)

    # BIVARIATE SORTS
    # Class BE/ME (value) and ME (size) bivariate sort
    pattern = r"^(\d+)_Portfolios_(\d+).*$"
    match = re.search(pattern, name)
    if match:
        print('matched US classic 2 way sorts')
        region = 'US'
        n = int(match.group(1))
        factors = ['BE/ME','ME']
        return (region, n, factors)
    
    # Bivariate sorts other than classic
    # US only

    # ones involving momentum first
    pattern = r"^(\d+)_Portfolios_([A-Z-]+)_Prior_(\d+)_(\d+)$"
    match = re.search(pattern, name)
    if match:
        print('matched US 2 way momentum sorts')
        reset_splits()
        region = 'US'
        n = int(match.group(1))
        sort_1 = match.group(2)
        sort_2 = 'Prior_' + match.group(3) + '_' + match.group(4)
        if n==25:
            sort_1_n = 5
            sort_2_n = 5
        elif n==6:
            sort_1_n = 2
            sort_2_n = 3    
        return (region, n, sort_1, sort_1_n, sort_2, sort_2_n, sort_3, sort_3_n)

    # the rest
    pattern = r"^(\d+)_Portfolios_([A-Z-]+)_([A-Z-]+)_(\d+)x(\d+)$"
    match = re.search(pattern, name)
    if match:
        print('matched US 2 way non-classic, non-momentum sorts')
        reset_splits()
        region = 'US'
        n = int(match.group(1))
        sort_1 = match.group(2)
        sort_2 = match.group(3)
        sort_1_n = int(match.group(4))
        sort_2_n = int(match.group(5))
        return (region, n, sort_1, sort_1_n, sort_2, sort_2_n, sort_3, sort_3_n)
    
    # Regional
    # momentum
    pattern = r"^([A-Za-z_]+)_(\d+)_Portfolios_([A-Z-]+)_Prior_(\d+)_(\d+)$"
    # pattern = r"^(\d+)_Portfolios_([A-Z-]+)_Prior_(\d+)_(\d+)*$"
    match = re.search(pattern, name)
    if match:
        reset_splits()
        region = match.group(1)
        n = int(match.group(2))
        sort_1 = match.group(3)
        sort_2 = 'Prior_' + match.group(3) + '_' + match.group(4)
        if n==25:
            sort_1_n = 5
            sort_2_n = 5
        elif n==6:
            sort_1_n = 2
            sort_2_n = 3    
        return (region, n, sort_1, sort_1_n, sort_2, sort_2_n, sort_3, sort_3_n)

    # the rest
    pattern = r"^([A-Za-z_]+)_(\d+)_Portfolios_([A-Z-]+)_([A-Z-]+)$"
    match = re.search(pattern, name)
    if match:
        print('matched regional momentum sorts')
        reset_splits()
        region = match.group(1)
        n = int(match.group(2))
        sort_1 = match.group(3)
        sort_2 = match.group(4)
        if n==4:
            sort_1_n = 2
            sort_2_n = 2
        elif n==6:
            sort_1_n = 2
            sort_2_n = 3
        elif n==25:
            sort_1_n = 5
            sort_2_n = 5        
        return (region, n, sort_1, sort_1_n, sort_2, sort_2_n, sort_3, sort_3_n)

    # Three way Regional sorts
    pattern = r"^([A-Za-z_]+)_(\d+)_Portfolios_([A-Z-]+)_([A-Z-]+)_(.*)_(\d+)x(\d+)x(\d+)$"    
    match = re.search(pattern, name)
    if match:
        print('matched 3 way regional sorts')
        reset_splits()
        region = match.group(1)
        n = int(match.group(2))
        sort_1 = match.group(3)
        sort_2 = match.group(4)
        sort_3 = match.group(5)        
        sort_1_n = int(match.group(6))
        sort_2_n = int(match.group(7))
        sort_3_n = int(match.group(8))
        return (region, n, sort_1, sort_1_n, sort_2, sort_2_n, sort_3, sort_3_n)

    # Industry Portfolios
    pattern = r"^(\d+)_Industry_Portfolios$"   
    match = re.search(pattern, name)
    if match:
        print('matched Industry Portfolios')
        region = 'US'
        reset_splits()
        n = int(match.group(1)) 
        sort_1 = 'Industry'
        sort_1_n = n
        return (region, n, sort_1, sort_1_n, sort_2, sort_2_n, sort_3, sort_3_n)    
    return None

# HERE - finished get_portfolio_inputs_from_filename - NEED TO TEST!!



if __name__ == "__main__":
    # extract_all_csvs()
    # name = 'North_America_32_Portfolios_ME_BE-ME_INV(TA)_2x4x4'
    # name = 'Portfolios_Formed_on_AC'
    # name = '25_Portfolios_OP_INV_5x5'
    # name = '38_Industry_Portfolios'
    # t=get_portfolio_inputs_from_filename(name)
    # print(t)
    # for elt in t:
    #     print(elt)

    start = timeit.timeit()
    load_all_data_into_duck_db()        
    end = timeit.timeit()
    print(end-start)

    # files = get_all_extracted_filenames()
    # print(files)

    ## works
    # f=determine_table_format('Portfolios_Formed_on_CF-P.CSV')
    # f=determine_table_format('100_Portfolios_ME_INV_10x10.CSV')    
    # f=determine_table_format('49_Industry_Portfolios.CSV')

    # not working
    # f=determine_table_format('North_America_6_Portfolios_ME_BE-ME.csv')   

    # f=determine_table_format('25_Portfolios_ME_AC_5x5.csv')   
    
    # r = parse_csv_file('Portfolios_Formed_on_CF-P.CSV')
    # r = parse_csv_file('Japan_32_Portfolios_ME_BE-ME_INV(TA)_2x4x4.csv')
    # print(r)
        
    
    
    # print(f)
