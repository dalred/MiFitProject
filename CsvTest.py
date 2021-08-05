# -*- coding: utf-8 -*-
import time,pandas,os, re
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe

folder_url = "D:\Downloads"
regex_csv = 'BODY_.*\.csv$'
SCOPES = ['https://www.googleapis.com/auth/drive']


gc = gspread.service_account(filename='C:/Users/dalre/PycharmProjects/csvproject/pythontableproject.json')
sht1 = gc.open_by_key('1igyXb2kspw0TCiUoNS9D4ZCmowRZ01RZIKRuhxC4iWQ')


def copy_pasta(path_to):
    list_ = ['timestamp', 'weight', 'fatRate', 'muscleRate']

    def date_parser(string_list):
        return [time.ctime(float(x)) for x in string_list]
    #Cчитываем csv
    data = pandas.read_csv(path_to, parse_dates=[0], sep=',', date_parser=date_parser)[list_]
    fatRate = data['fatRate'] > 0
    muscleRate = data['muscleRate'] > 0
    data['Muscle percentage']= data['muscleRate']/data['weight']

    #Новый dataframe
    df_new = data[fatRate & muscleRate]

    ws = sht1.worksheet('Weight_Musclerate')

    #Добавляем в лист так как в исходном листе нет, так как в csv изначальном - нет
    list_.append('Muscle percentage')
    # Считываем исходный dataframe с гугла
    df_old = get_as_dataframe(ws, parse_dates=True)
    existing = df_old[list_].dropna(how='all')
    #Складываем вместе dataframe старый и новый
    updated = existing.append(df_new)
    set_with_dataframe(ws,updated)

    ws.format('A2:A', {'numberFormat': {'type': 'DATE','pattern': 'dd.MM.yyyy'}})
    ws.format('B2:D', {'numberFormat': {'type': 'NUMBER','pattern': '0.00'}})
    ws.format('E2:E', {'numberFormat': {'type': 'PERCENT','pattern': '00.00%'}})
    ws.format('A2:E',{"horizontalAlignment": 'CENTER','textFormat': {'fontSize': '10'}})


def read_csv():
    filtered_csv = []
    for root, dirs, files in os.walk(folder_url):
        # del dirs[:]  # go only one level deep
        for i in files:
            if re.search(regex_csv, str(i)):
                filtered_csv.append(str(root).replace('\\', '/') + "/" + str(i))
    return filtered_csv



for i in read_csv():
    copy_pasta(i)
    os.remove(i)