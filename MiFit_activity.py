# -*- coding: utf-8 -*-
import pandas,os, re,shutil
import gspread
from gspread_dataframe import set_with_dataframe


folder_url = "D:\Downloads"
regex_csv = 'ACTIVITY_(?!STAGE|MINUTE).*\.csv$'
SCOPES = ['https://www.googleapis.com/auth/drive']


gc = gspread.service_account(filename='C:/Users/dalre/PycharmProjects/csvproject/pythontableproject.json')
sht1 = gc.open_by_key('')

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

def copy_pasta(path_to):
    list_ = ['date', 'steps', 'calories']
    data = pandas.read_csv(path_to, parse_dates=[0], sep=',')[list_]
    #Новый dataframe
    ws = sht1.worksheet('Activity')
    max_rows = len(ws.get_all_values())
    set_with_dataframe(ws, data, row=max_rows+1, col=1, include_column_header=False)
    ws.format('A2:A', {'numberFormat': {'type': 'DATE','pattern': 'dd.MM.yyyy'}})
    ws.format('B2:E', {'numberFormat': {'type': 'NUMBER','pattern': '0'}})

if len(path_to_csv(folder_url,regex_csv)) > 0:
    for i in path_to_csv(folder_url,regex_csv):
        print('Записываю ' + i)
        copy_pasta(i)
        path = i.split("/")
        path_join = '/'.join([path[0], path[1], path[2]])
        path_zip = path_join + '.zip'
        if os.path.isfile(path_zip):
            print('Удаляю zip: '+path_zip)
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