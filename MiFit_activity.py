# -*- coding: utf-8 -*-
import pandas, os, re, shutil
import gspread
from gspread_dataframe import set_with_dataframe, get_as_dataframe
from dotenv import dotenv_values
from datetime import datetime

config = dotenv_values(".env")
folder_url = "D:\Downloads"
regex_csv = 'ACTIVITY_(?!STAGE|MINUTE).*\.csv$'
SCOPES = ['https://www.googleapis.com/auth/drive']

gc = gspread.service_account(filename='C:/Users/dalre/PycharmProjects/csvproject/pythontableproject.json')
sht1 = gc.open_by_key(config.get('GC_KEY'))

ws1 = sht1.worksheet('Activity')
# ws1 = sht1.worksheet('Activity_test')
ws2 = sht1.worksheet('Avg_activity_weeks')

list_ = ['date', 'steps', 'calories']
# old
# list_csv = ['date','datetimestamp',  'steps', 'distance', 'runDistance', 'calories']
list_csv = ['date', 'steps', 'distance', 'runDistance', 'calories']
# custom_date_parser = lambda x: datetime.utcfromtimestamp(int(x)).strftime('%Y-%m-%d %H:%M:%S')
custom_date_parser = lambda x: datetime.strptime(x, "%Y-%m-%d")


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


def paste_empty_row(index: int, number_of_rows: int, worksheet) -> None:
    # worksheet.col_count - считает неверно ставим по умолчанию 3
    # empty_row = ['' for cell in range(worksheet.col_count)]
    empty_rows = [['']*3 for _ in range(number_of_rows)]
    # TODO Проверить insert_row с inherit_from_before
    # Не подходит под наш случай
    # https://docs.gspread.org/en/latest/api/models/worksheet.html?highlight=append#gspread.worksheet.Worksheet.append_row
    worksheet.insert_rows(empty_rows, row=index, value_input_option='RAW')


def avg_weeks():
    # Перезапись
    print('Cчитываю старый Dataframe c листа!')
    df_old = get_as_dataframe(ws1, parse_dates=True, usecols=[0, 1, 2])
    # Удаление
    df_old = df_old[list_].dropna(how='all')
    df_old['dateAvg'] = pandas.to_datetime(df_old['date'], format="%d.%m.%Y")
    df_old = df_old.set_index('dateAvg')
    df_old = df_old.resample('W').mean()
    df_old = df_old.sort_values('dateAvg', ascending=False)
    set_with_dataframe(ws2, df_old, row=2, col=1, include_column_header=False, include_index=True)
    # TODO Разобраться с типами
    format_table(worksheet=ws2, number=len(df_old)+1)


def copy_pasta(path_to):
    data = pandas.read_csv(path_to, sep=',', parse_dates=[0], date_parser=custom_date_parser, header=None, skiprows=1)
    # Присваиваем новый header
    data.columns = list_csv
    # Забираем нужные колонки
    data = data[list_]
    data = data.sort_values('date', ascending=False)
    # Новый dataframe
    # Вставка пустых строк
    paste_empty_row(index=2, number_of_rows=len(data), worksheet=ws1)
    set_with_dataframe(ws1, data, row=2, col=1, include_column_header=False)
    format_table(worksheet=ws1)


def format_table(worksheet, number=''):
    worksheet.format('A2:A'+str(number),
                     {'numberFormat':
                          {'type': 'DATE', 'pattern': 'dd.MM.yyyy'},
                      })
    worksheet.format('B2:E'+str(number),
                     {'numberFormat':
                          {'type': 'NUMBER', 'pattern': '0'}
                      })
    worksheet.format('A2:C'+str(number),
                     {
                         'horizontalAlignment': 'CENTER',
                         'textFormat': {'fontSize': '9'},
                         'verticalAlignment': 'MIDDLE',
                         'wrapStrategy': 'WRAP'
                     })


if len(path_to_csv(folder_url, regex_csv)) > 0:
    for i in path_to_csv(folder_url, regex_csv):
        print('Записываю ' + i)
        copy_pasta(i)
        avg_weeks()
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
