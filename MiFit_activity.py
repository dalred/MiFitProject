# -*- coding: utf-8 -*-
import pandas,os, re,shutil
import gspread
from gspread_dataframe import set_with_dataframe,get_as_dataframe


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

def avg_weeks():
    print('Cчитываю старый Dataframe c листа!')
    df_old = get_as_dataframe(ws, parse_dates=True, usecols=[0,1,2])
    df_old = df_old[list_].dropna(how='all')
    df_old['dateAvg'] = pandas.to_datetime(df_old['date'], format="%d.%m.%Y")
    df_old = df_old.set_index('dateAvg')
    df_old = df_old.resample('W').mean()
    #print(df)
    set_with_dataframe(ws, df_old, row=2, col=4, include_column_header=False,include_index=True)

def copy_pasta(path_to):
    data = pandas.read_csv(path_to, parse_dates=[0], sep=',')[list_]
    #Новый dataframe
    max_rows = len(ws.get_all_values())
    next_row = max_rows + 1
    end_row = next_row+len(data)
    set_with_dataframe(ws, data, row=max_rows+1, col=1, include_column_header=False)
    ws.format('A2:A',
              {'numberFormat':
                   {'type': 'DATE', 'pattern': 'dd.MM.yyyy'},
               })
    ws.format('D2:D',
              {'numberFormat':
                   {'type': 'DATE', 'pattern': 'dd.MM.yyyy'},
               })
    ws.format('B2:E',
              {'numberFormat':
                   {'type': 'NUMBER', 'pattern': '0'}
               })
    ws.format('A' + str(next_row) + ':C' + str(end_row),
              {
                  'horizontalAlignment': 'CENTER',
                  'textFormat': {'fontSize': '9'},
                  'verticalAlignment': 'MIDDLE',
                  'wrapStrategy': 'WRAP'
              })

def format_table_two():
    ws.format('D2:D',
              {'numberFormat':
                   {'type': 'DATE', 'pattern': 'dd.MM.yyyy'},
               })
    ws.format('E2:F',
              {'numberFormat':
                   {'type': 'NUMBER', 'pattern': '0'}
               })
    ws.format('D2:F',
              {
                  'horizontalAlignment': 'CENTER',
                  'textFormat': {'fontSize': '9'},
                  'verticalAlignment': 'MIDDLE',
                  'wrapStrategy': 'WRAP'
              })


if len(path_to_csv(folder_url,regex_csv)) > 0:
    for i in path_to_csv(folder_url,regex_csv):
        print('Записываю ' + i)
        copy_pasta(i)
        avg_weeks()
        format_table_two()
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
