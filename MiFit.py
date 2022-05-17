# -*- coding: utf-8 -*-
import time, pandas, os, re, shutil
import gspread
from gspread_dataframe import set_with_dataframe
from datetime import datetime
from dotenv import dotenv_values
import pytz

config = dotenv_values(".env")
folder_url = "D:\Downloads"
regex_csv = 'BODY_.*\.csv$'
SCOPES = ['https://www.googleapis.com/auth/drive']

gc = gspread.service_account(filename='C:/Users/dalre/PycharmProjects/csvproject/pythontableproject.json')
sht1 = gc.open_by_key(config.get('GC_KEY'))
tz = pytz.timezone('Europe/Moscow')

def copy_pasta(path_to):
    list_csv = ['time', 'weight', 'height', 'bmi', 'fatRate', 'bodyWaterRate', 'boneMass', 'metabolism', 'muscleRate',
                'visceralFat', 'unname']
    list_ = ['time', 'weight', 'fatRate', 'muscleRate']

    #custom_date_parser = lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S+0000")
    custom_date_parser = lambda x: datetime.fromtimestamp(int(x), tz).strftime('%Y-%m-%d %H:%M:%S')

    # Cчитываем csv
    data = pandas.read_csv(path_to, sep=',', parse_dates=[0], date_parser=custom_date_parser, header=None, skiprows=1)
    data.columns = list_csv
    data = data[list_]
    fatRate = data['fatRate'] > 0
    muscleRate = data['muscleRate'] > 0
    # Добавляем еще колонку (Column)
    data['Muscle percentage'] = data['muscleRate'] / data['weight']
    data['fatRate'] = data['fatRate'] / 100
    # Берем только те значения кто больше 0, на всякий случай.
    data = data[fatRate & muscleRate]
    data = data.sort_values('time')  # В CSV почему-то стало наоборот по убыванию
    ws = sht1.worksheet('Weight_Musclerate')
    max_rows = len(ws.get_all_values())
    set_with_dataframe(ws, data, row=max_rows + 1, col=1, include_column_header=False)

    ws.format('A2:A', {'numberFormat': {'type': 'DATE', 'pattern': 'dd.MM.yyyy'}})
    ws.format('B2:B', {'numberFormat': {'type': 'NUMBER', 'pattern': '0.00'}})
    ws.format('C2:C', {'numberFormat': {'type': 'PERCENT', 'pattern': '00.00%'}})
    ws.format('D2:D', {'numberFormat': {'type': 'NUMBER', 'pattern': '0.00'}})
    ws.format('E2:E', {'numberFormat': {'type': 'PERCENT', 'pattern': '00.00%'}})
    ws.format('A2:E', {"horizontalAlignment": 'CENTER', 'textFormat': {'fontSize': '10'}})


def path_to_csv(folder_url, regex_csv):
    filtered_csv = []
    for root, dirs, files in os.walk(folder_url):
        # del dirs[:]  # go only one level deep
        for i in files:
            if re.search(regex_csv, str(i)):
                path = str(root).replace('\\', '/') + "/" + str(i)
                os.chmod(path, 0o777)
                filtered_csv.append(path)
    return filtered_csv


if len(path_to_csv(folder_url, regex_csv)) > 0:
    for i in path_to_csv(folder_url, regex_csv):
        print('Записываю ' + i)
        copy_pasta(i)
        path = i.split("/")
        path_join = '/'.join([path[0], path[1], path[2]])
        path_zip = path_join + '.zip'
        if os.path.isfile(path_zip):
            print('Удаляю zip: ' + path_zip)
            os.remove(path_zip)
        if len(path) == 3:
            # Проверяем если это файл
            if os.path.isfile(path_join):
                print('Удаляю: ' + path_join)
                os.remove(path_join)
        else:
            # Если это папка
            print('Удаляю: ' + path_join)
            shutil.rmtree(path_join)
else:
    print("Файлы отсутствуют!")
