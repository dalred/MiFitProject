# -*- coding: utf-8 -*-
import os, re
folder_url="D:\Downloads"
regex_csv = 'BODY_.*\.csv$'
filtered_csv=[]
for root, dirs, files in os.walk(folder_url):
    #del dirs[:]  # go only one level deep
    for i in files:
        if re.search(regex_csv, str(i)):
            filtered_csv.append(str(root).replace('\\','/') + "/" + str(i))



os.remove(filtered_csv[0])

