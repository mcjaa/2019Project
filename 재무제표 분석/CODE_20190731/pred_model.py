import pandas as pd
import numpy as np
import pickle
import sklearn
import requests
import json
import re
from bs4 import BeautifulSoup as bs
import tensorflow as tf

class preprocess:

    def __init__(self):
        print('init')

    def data_crawlling(self, code):

        url = "https://wisefn.finance.daum.net/v1/company/cF1001.aspx?cmp_cd=" + code + "&frq=0&rpt=1&finGubun=MAIN"
        html = requests.get(url).text

        # 컬럼명 : 기준년월 추출
        match1 = re.search(r'var changeFin = (.+?);', html, re.S)
        json_string1 = match1.group(1)
        col_name = json.loads(json_string1)

        col_name = col_name[1][2:]

        q_name = ['type']
        for i in range(0, len(col_name)):
            q_name.append(col_name[i][:7])

        # 재무데이터 추출
        match2 = re.search(r'var changeFinData = (.+?);', html, re.S)
        json_string2 = match2.group(1)
        fin_data = json.loads(json_string2)

        q_data = fin_data[0][1] + fin_data[1][1] + fin_data[2][1] + fin_data[3][1]

        col_names = pd.DataFrame(fin_data[0][0] + fin_data[1][0] + fin_data[2][0] + fin_data[3][0]).iloc[:, 0]
        q_df = pd.concat([col_names, pd.DataFrame(q_data).iloc[:, -2:]], axis=1)

        q_df.columns = q_name

        target_date = col_name[1][:7]
        print(target_date)
        return q_df, target_date

    def list_crawlling(self):
        stock_list = pd.read_csv("stock_df_compl.csv", converters={'종목코드': str})
        code_list = stock_list['종목코드'].tolist()
        price_df_monthly = pd.DataFrame()

        # 재무제표 크롤링 함수 수행
        i = 0
        for code_i in code_list:

            # 데이터 크롤링
            q_df, target_date = self.data_crawlling(code_i)

            # Price Collecting 크롤링
            price_df_monthly = price_df_monthly.append(self.price_crawling(code_i, "60"))

            # 분기데이터
            if i == 0:
                qauter_data = self.make_table(q_df, code_i)
                i = 1
            else:
                qauter_data = pd.concat([qauter_data, self.make_table(q_df, code_i)])

        return qauter_data, price_df_monthly

    def make_table(self, df, code):

        # 컬럼필터링 & Transpos
        temp_data_T = df.T

        # 종목컬럼추가
        temp_data_T.columns = temp_data_T.iloc[0]
        temp_data_T = temp_data_T.drop("type", axis=0)
        temp_data_T['code'] = [code] * len(temp_data_T)
        col_1 = temp_data_T.columns.tolist()

        col_2 = col_1[-1:] + col_1[:-1]
        temp_data_T = temp_data_T[col_2]
        temp_data_T
        temp_data_T = temp_data_T.reset_index()
        #     temp_data_T = temp_data_T.reset_index(drop = True)
        temp_data_T = temp_data_T.rename(columns={'index': 'DATE'})

        return temp_data_T

    def price_crawling(self, code, n_line):

        url = "https://fchart.stock.naver.com/sise.nhn?symbol=" + code + "&timeframe=month&count=" + n_line + "&requestType=0"
        html = requests.get(url).text

        # item 태크로 찾기
        data_to_parse = bs(html)
        item_data = data_to_parse.find_all("item")

        data_list = []

        # data 속성으로 찾기
        for i in item_data:
            data_list.append(i.get("data"))

        # delemiter로 slpit 하기
        for i in range(0, len(data_list)):
            data_list[i] = data_list[i].split("|")

        data_df = pd.DataFrame(data_list)

        # 필요없는 column들은 제외해준다.
        data_df = data_df[[0, 4, 5]]
        data_df['code'] = code

        # 컬럼명 바꿔준다.
        data_df = data_df.rename(columns={0: 'DATE', 4: '종가', 5: '거래량'})

        return data_df

    def add_col(self, quater_data, price_df):

        quater_data.columns = ['DATE', 'code', 'N1', 'N2', 'N3', 'N4', 'N5', 'N6', 'N7', 'N8', 'N9', 'N10', 'N11',
                               'N12', 'N13', 'N14', 'N15', 'N16', 'N17', 'N18', 'N19'
            , 'R1', 'R2', 'R3', 'R4', 'R5', 'R6', 'N20', 'R7', 'N21', 'R8', 'N22', 'R9', 'R10', 'COUNT']

        quater_data.drop(['N6', 'N7', 'N11', 'N12', 'N18', 'N22', 'R9', 'R10'], axis='columns', inplace=True)

        for i in range(0, len(quater_data)):
            quater_data.loc[i, :] = quater_data.loc[i, :].apply(str).str.replace(',', '')
            quater_data.loc[i, 'DATE'] = quater_data.loc[i, 'DATE'].replace('/', '')

        # 추가 필요 컬럼 생성
        quater_data['VALUE'] = ""
        quater_data['PRICE'] = ""

        quater_data['F3'] = ''  # ROA 변동
        quater_data['F4'] = ''  # 당기순이익과 영업현금흐름 비교
        quater_data['F5'] = ''  # 부채비율 변동
        # quater_data_labeled2['F6'] = '' # 유동성 상승여부 - 현재 계산가능 데이터 크롤링 안함
        quater_data['F7'] = ''  # 자기자본 문제
        quater_data['F8'] = ''  # 매출총이익률 상승여부
        quater_data['F9'] = ''  # 자산 회전율 상승여부

        # 가격정보 세팅
        for i in range(0, len(quater_data)):

            for ii in range(0, len(price_df)):
                if price_df.loc[ii, 'DATE'][:6] == quater_data.loc[i, 'DATE']:
                    quater_data.loc[i, "PRICE"] = price_df.loc[ii, "종가"]
                    quater_data.loc[i, "VALUE"] = float(quater_data.loc[i, "COUNT"]) * float(price_df.loc[ii, "종가"])

                    break

        prev_data = quater_data.loc[0, :]
        target_data = quater_data.loc[1, :]

        if float(prev_data['R4']) != 0:
            target_data['F3'] = float(target_data['R4']) / float(prev_data['R4'])
        if float(prev_data['R5']) != 0:
            target_data['F5'] = float(target_data['R5']) / float(prev_data['R5'])

        target_data['F4'] = float(target_data['N14']) - float(target_data['N5'])
        target_data['F7'] = float(target_data['COUNT']) - float(prev_data['COUNT'])
        target_data['F8'] = float(target_data['R1']) - float(prev_data['R1'])
        target_data['F9'] = float(target_data['N1']) / float(target_data['N8']) - float(prev_data['N1']) / float(
            prev_data['N8'])

        # 당기순이익
        if float(target_data['N5']) > 0:
            target_data['S1'] = 1
        else:
            target_data['S1'] = 0

        # 영업현금흐름
        if float(target_data['N14']) > 0:
            target_data['S2'] = 1
        else:
            target_data['S2'] = 0

        # 전년대비 ROA증가

        try:
            if float(target_data['F3']) > 1:
                target_data['S3'] = 1
            else:
                target_data['S3'] = 0
        except:
            print(i)

        # 영업현금흐름 > 순이익
        if float(target_data['F4']) > 0:
            target_data['S4'] = 1
        else:
            target_data['S4'] = 0

        # 전년대비 부채비율 감소여부
        if float(target_data['F5']) < 1:
            target_data['S5'] = 1
        else:
            target_data['S5'] = 0

        # 전년대비 유동비율 증가여부
        if float(target_data['R5']) - float(prev_data['R5']) > 0:
            target_data['S6'] = 1
        else:
            target_data['S6'] = 0

        # 당해 신규주식발행여부
        if float(target_data['F7']) > 0:
            target_data['S7'] = 0
        else:
            target_data['S7'] = 1

        # 전년대비 매출총이익 증가여부
        if float(target_data['F8']) > 0:
            target_data['S8'] = 1
        else:
            target_data['S8'] = 0

        # 전년대비 자상회전율 증가여부
        if float(target_data['F9']) > 0:
            target_data['S9'] = 1
        else:
            target_data['S9'] = 0

        quater_data = quater_data.drop([1])
        quater_data = quater_data.append(target_data)

        return quater_data

    def dealing_nullValue(self, quater_data):
        quater_data = quater_data.loc[-quater_data.isnull()["N1"], :]
        quater_data = quater_data.loc[-quater_data.isnull()["N16"], :]
        quater_data = quater_data.loc[-quater_data.isnull()["N17"], :]
        # quater_data = quater_data.loc[-quater_data.isnull()["N19"],:]
        quater_data = quater_data.loc[-quater_data.isnull()["R3"], :]
        quater_data = quater_data.loc[-quater_data.isnull()["R7"], :]
        quater_data = quater_data.loc[-quater_data.isnull()["VALUE"], :]
        # quater_data = quater_data.loc[-quater_data.isnull()["PRICE"],:]
        quater_data = quater_data.loc[-quater_data.isnull()["F3"], :]

        quater_data.drop(['N19'], axis='columns', inplace=True)

        print(len(quater_data))
        print(quater_data.isnull().sum())

        # 분기 Dummy 변수 만들어주기 ( 코드보완필요!!)
        quater_data = quater_data.drop([0])
        quater_data = quater_data.reset_index(drop=True)
        quater_data["Q02"] = "0"
        quater_data["Q03"] = "0"
        quater_data["Q08"] = "0"
        quater_data["Q09"] = "0"
        quater_data["Q11"] = "0"
        quater_data["Q12"] = "0"

        quater = "Q" + quater_data.loc[0, "DATE"][4:6]
        quater_data[quater] = "1"

        return quater_data

    def call_scale(self, quater_data, scaler):
        col_name = ['N1', 'N2', 'N3', 'N4', 'N5', 'N8', 'N9', 'N10', 'N13',
                'N14', 'N15', 'N16', 'N17', 'R1', 'R2', 'R3', 'R4', 'R5', 'R6',
                'N20', 'R7', 'N21', 'R8', 'COUNT', 'VALUE', 'F3', 'F4', 'F5', 'F7', 'F8', 'F9']

        # Scaler 로딩하기
        scaler = pickle.load(open(scaler + ".sav", 'rb'))
        std_df = scaler.transform(quater_data[col_name])
        std_df = pd.DataFrame(std_df, columns=col_name)

        return std_df

    def add_col2(self, scale_data, clean_data):

        new_df = pd.merge(scale_data, clean_data[["S1", "S2", "S3", "S4", "S5", "S6", "S7", "S8", "S9",
                                                  'Q02', 'Q03', 'Q08', 'Q09', 'Q11', 'Q12']], left_index=True,
                          right_index=True)
        new_df["SS"] = new_df["S1"] + new_df["S2"] + new_df["S3"] + new_df["S4"] + new_df["S5"] + new_df["S6"] + new_df[
            "S7"] + new_df["S8"] + new_df["S9"]
        new_df["SL"] = new_df["SS"].apply(lambda x: 1 if x > 4 else 0)
        new_df['R8_2'] = pow(new_df['R8'], 2)
        new_df['R8_3'] = pow(new_df['R8'], 3)

        return new_df

    def model_load(self, my_model, new_df):

        ## 학습된 모델 로딩하기
        model = tf.keras.models.load_model(my_model)
        pred = pd.DataFrame(model.predict(new_df))
        print(pred)

        return pred