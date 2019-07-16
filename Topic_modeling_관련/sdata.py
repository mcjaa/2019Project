import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup as bs
import time
import random
import telepot
import re
import json

from datetime import datetime, timedelta
import matplotlib.pyplot as plt



def mylist() :
    msg = """
    make_daily_price_table() : 전체 종목의 주가표를 그려주는 함수
    
    make_daily_price_table1() : 전체 종목의 주가표를 그려주는 함수(영문코드)
    
    get_price_data(daily_price_table, code, item) : 특정 종목의 특정 항목 시계열 데이터를 보여주는 함수
    
    investing_get_historical_data(code, start_date) : investing.com에서 특정 일자 이후 종목 주가 데이터를 가져오는 함수
    
    get_current_table() : 전종목 현재가 불러오기

    get_all_code() : 전종목 코드 불러오기
    
    find_code(company) : 종목명으로 종목코드 찾기
    
    set_mat_font() : font 사용
    
    get_current_time() : 현재 시각 문자로 구하기
    
    preprocessing(text) : 전처리
   
    """
    print(msg)




# ### 전종목 일간 데이터 나타내는 함수

# In[139]:


def make_daily_price_table() :
    df=pd.read_csv("https://drive.google.com/uc?export=download&id=1d5MypCnKIGKYfto6JTddaRCZDc54eH-J", index_col=[0,2], encoding='euckr')
    price_table = df.T.iloc[1:]
    price_table.index = pd.to_datetime(price_table.index)
    price_table.columns = price_table.columns.swaplevel(0,1)
    columns = {
               '수정주가(원)' : '종가',
               '수정시가(원)' : '시가',
               '수정고가(원)' : '고가',
               '수정저가(원)' : '저가',
               '거래량(주)' : '거래량',
               '매수수량(기관계)(주)' : '기관매수',
               '매도수량(기관계)(주)' : '기관매도',
               '매도수량(외국인계)(주)' : '외인매도',
               '매도수량(외국인계)(주)' : '외인매수'
              }
    price_table = price_table.rename(columns=columns)
    
    return  price_table

def make_daily_price_table1() :
    df=pd.read_csv("https://drive.google.com/uc?export=download&id=1d5MypCnKIGKYfto6JTddaRCZDc54eH-J", index_col=[0,2], encoding='euckr')
    price_table = df.T.iloc[1:]
    price_table.index = pd.to_datetime(price_table.index)
    price_table.columns = price_table.columns.swaplevel(0,1)
    columns = {
               '수정주가(원)' : 'close',
               '수정시가(원)' : 'open',
               '수정고가(원)' : 'high',
               '수정저가(원)' : 'low',
               '거래량(주)' : 'volume',
               '매수수량(기관계)(주)' : 'inv_buy',
               '매도수량(기관계)(주)' : 'inv_sell',
               '매수수량(외국인계)(주)' : 'for_buy',
               '매도수량(외국인계)(주)' : 'for_sell'
              }
    price_table = price_table.rename(columns=columns)
        
    return  price_table


def get_price_data(daily_price_table, code, item) :
    return daily_price_table[item][code]




def investing_get_historical_data(code, start_date) :

    # 기업 인베스팅닷컴 고유 아이디를 구하기
    url = 'https://www.investing.com/search/service/search'
    headers = {
            'User-Agent':'Mozilla',
            'X-Requested-With':'XMLHttpRequest',
        }
    
    datas = {
        'search_text': code,
        'term': code,
        'country_id': '0',
        'tab_id': 'All'
        }
    res = requests.post(url, data=datas, headers=headers)
    
    
    # 해당 데이에서 curr_id 구하기
    code_dict = json.loads(res.text)
    
   
    curr_id = str(list(filter(lambda x : x['symbol'] == code, code_dict['All']))[0]['pair_ID'])
    
    today = datetime.strftime(datetime.utcnow() + timedelta(hours = 9), "%m/%d/%Y")
    
    
    # 주가가져오기

    url_data = 'https://www.investing.com/instruments/HistoricalDataAjax'
    
    datas = {    
                'curr_id': curr_id,   # 앞에서 구한 기업 고유 아이디
                'st_date': start_date,  # 함수에서 입력받은 날짜
                'end_date': today,   # 오늘날짜(변경가능)
                'interval_sec':'Daily',
                'sort_col':'date',
                'sort_ord':'ASC',
                'action':'historical_data'
            }

    headers = {
            'User-Agent':'Mozilla',
            'X-Requested-With':'XMLHttpRequest',
            }

    res_data = requests.post(url_data, data=datas, headers=headers)
    data = pd.read_html(res_data.text)[0] 
    data['Date'] = pd.to_datetime(data['Date'])
    data = data.set_index('Date')
    data['symbol'] = code
    
    return data[::-1]    # 데이터 데이블 받기 



def get_current_table() :

    # str형태로 되어 있으며, json 양식에 맞도록 몇몇 문장이나 부호를 제거하고 json 형식으로 바꾸려고 함
    # 코스닥, 코스피 url이 따로 되어 있기 때문에 각각 불러오려고 함 


    # json으로 바꾸기 전단계에 지워야할 항목들
    remove_list = ['\n', '\t', 'var dataset =', ';']
    quote_list = ['"timeinfo', 'date', 'time', 'message', 'kospi', 'cost', 'updn', 'rate', 'kosdaq', 'list', 'upjong', 'name', 'code', 'avg', 'item']



    current_table = pd.DataFrame()  # 결과 DataFrame

    # 코스피에서 불러오기

    url = "http://finance-service.daum.net/xml/xmlallpanel.daum?stype=P&type=S"
    r1 = requests.get(url)

    json_str = r1.text



    for word in remove_list:
        json_str = json_str.replace(word, '')

    for word in quote_list:
        json_str = json_str.replace(word, '"{0}"'.format(word))

    json_str = json_str.replace('"time"info', '"timeinfo"')
    print(json_str)
    

    json_str = json.loads(json_str)['item']



    kospi_list = []
    
    for item in json_str :
        kospi_list.append(item)
    
    temp_df = pd.DataFrame(kospi_list)
    temp_df['market'] = 'KOSPI'
    current_table = current_table.append(temp_df)
    

    # 코스닥에서 불러오기

    url2 = 'http://finance-service.daum.net/xml/xmlallpanel.daum?stype=Q&type=S'
    r2 = requests.get(url2)
    json_str2 = r2.text

    for word in remove_list:
        json_str2 = json_str2.replace(word, '')

    for word in quote_list:
        json_str2 = json_str2.replace(word, '"{0}"'.format(word))

        
       
    json_str2 = json.loads(json_str2.replace('"time"info', '"timeinfo"'))['item']

    
    kosdaq_list= []
    for item in json_str2 :
        kosdaq_list.append(item)

    temp_df = pd.DataFrame(kosdaq_list)
    temp_df['market'] = 'KOSDAQ'
    current_table = current_table.append(temp_df)
    
    current_table = current_table.rename(columns = {'cost' : 'price', 'name' : 'company'})
    
    def to_num(data):
        return int(data.replace(",",""))
    
    def make_code(data) :
        return "A"+data
    
    current_table['price'] = current_table['price'].apply(to_num)
    current_table['code'] = current_table['code'].apply(make_code)
    
    return current_table[['code', 'company', 'market', 'price', 'rate', 'updn']]



# In[72]:



# ### 전종목 코드 구하기

# In[93]:


def get_all_code() :
    
    market_list = [('stockMkt','KOSPI'), ('kosdaqMkt', 'KOSDAQ'), ('konexMkt', 'KONEX')]
    
    code = pd.DataFrame()
    
    for market in market_list :
        url = 'http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13&marketType='+market[0]
        code_df = pd.read_html(url, header=0)[0]
        code_df['code'] = code_df['종목코드'].apply(lambda x: '{:06d}'.format(x))
        code_df['market'] = market[1]
        code_df = code_df[['code', '회사명', 'market']]
        code = code.append(code_df)
        
    code.columns = ['code', 'company', 'market']
    
    def to_code(data) :
        return "A" + data
    
    code['code'] = code['code'].apply(to_code)
    
    return code


# In[95]:


# ### 종목명으로  코드 가져오기

# In[98]:


def find_code(company) :
    url = 'http://marketdata.krx.co.kr/WEB-APP/autocomplete/autocomplete.jspx'
    params = {
        'contextName': 'stkisu3',
        'value': company,
        'viewCount': '5',
        'bldPath': '/COM/finder_stkisu_autocomplete'
        }
    res = requests.get(url, params = params )
    info_list = bs(res.text, 'html.parser').findAll('li')
    
    return list(filter(lambda x : x['data-nm'] == company, info_list))[0]['data-tp']
    

    
def set_mat_font() :

    import platform
    #from matplotlib import font_manager, rc
    from matplotlib import font_manager as fm

    # 폰트 변경시 마이너스 깨지는 문제 수정
    plt.rcParams['axes.unicode_minus'] = False 

    font_size = 12   # font size 입력
    win_path = "c:/Windows/Fonts/malgun.ttf"

    # 시스템에 저장되어있는 고딕폰트 목록 가져오기
    gothic_fonts = [(f.name, f.fname) for f in fm.fontManager.ttflist if 'Gothic' in f.name]
    print(gothic_fonts)

    # plt전역에 폰트 지정
    if platform.system()=='Darwin':
        plt.rcParams['font.family'] = "AppleGothic"
    elif platform.system() == 'Windows' :
        plt.rcParams['font.family'] = "Malgun Gothic"
    elif platform.system() == 'Linux':
        font_dirs = ['/home/nbuser/fonts', ]
        # System font 찾는 경로를 설정
        font_files = fm.findSystemFonts(fontpaths=font_dirs)
        # 폰트 파일로 부터 폰트 리스트를 생성
        font_list = fm.createFontList(font_files)
        print(font_list)
        # matplotlib fontManager의 ttflist에 생성한 폰트리스트를 추가
        fm.fontManager.ttflist.extend(font_list)
        plt.rcParams['font.family'] = "NanumGothic"
    else :

        print('Unknown')

    plt.rcParams["font.size"] = font_size



def get_current_time():
    return datetime.strftime(datetime.utcnow()+timedelta(hours=9), '%Y-%m-%d %H:%M')



def preprocessing(text) :
    return text.replace("\n","").replace('\t', '').replace('\r', '')
