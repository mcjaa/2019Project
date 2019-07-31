from PyQt5 import QtGui, uic, QtWidgets
from PyQt5.QtWidgets import QPushButton
from PyQt5.QtWidgets import QMainWindow, QApplication
from PyQt5.QtWidgets import *
from PyQt5.QtCore import *
from PyQt5.QtGui import QIcon, QPixmap
import sys
import tensorflow as tf

import pandas as pd
import numpy as np
import pickle
import sklearn
import requests
import json
import re
from bs4 import BeautifulSoup as bs

import pred_model



class Form(QtWidgets.QDialog):
    def __init__(self, parent=None):
        QtWidgets.QDialog.__init__(self, parent)
        self.ui = uic.loadUi("0729_1.ui")
        self.ui.show()

        self.ui.pushButton.clicked.connect(self.clickMethod)

    def clickMethod(self):
        item_code = self.ui.textEdit  # ui에 저장된 오브젝트
        print(self.ui.textEdit.toPlainText())
        #         self.showimage(textEdit, 0)
        QMessageBox.about(self.ui, "message", "기업분석을 시작합니다.")
        instance = pred_model.preprocess()

        input_code = self.ui.textEdit.toPlainText()
        no_price = "12"
        model_date = "0726"
        scaler = "scaler_3"
        my_model = "my_model.h5"

        q_df, target_date = instance.data_crawlling(input_code)
        quater_data = instance.make_table(q_df, input_code)
        price_df = instance.price_crawling(input_code, no_price)
        quater_data2 = instance.add_col(quater_data, price_df)
        clean_data = instance.dealing_nullValue(quater_data2)
        scale_data = instance.call_scale(clean_data, scaler)
        new_df = instance.add_col2(scale_data, clean_data)
        pred_result = instance.model_load( my_model, new_df)

        print(pred_result)
        print("*******************")
        print(pred_result.iloc[0,0])
        pctg_val = pred_result.iloc[0, 0] * 100
        pctg_val = round(pctg_val,5)
        text_val = target_date + " 분기 재무제표를 기준으로 3개월 이후 해당종목 주가 상승확률은 \n" + str(pctg_val) + "% 입니다."
        self.ui.progressBar.setValue(int(pctg_val))
        self.ui.textBrowser.setText(text_val)  # 내용 입력

    # def clickListMethod(self):
    #     instance = pred_model.preprocess()
    #     quater_data = instance.list_crawlling()
    #
    #     quater_data2 = instance.add_col(quater_data, price_df)
    #     clean_data = instance.dealing_nullValue(quater_data2)
    #     scale_data = instance.call_scale(clean_data, scaler)
    #     new_df = instance.add_col2(scale_data, clean_data)
    #     pred_result = instance.model_load(my_model, new_df)

if __name__ == '__main__':
    app = QtWidgets.QApplication(sys.argv)
    w = Form()
    sys.exit(app.exec())