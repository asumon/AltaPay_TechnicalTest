# Change the code to process only files starting with '82242267'
# and ending with '0000'. The files can be found in "data" folder

import os
import fnmatch
import csv

files = os.listdir(
    '../data/')

data = ''

for file in files:
    if fnmatch.fnmatch(file, '82242267*0000.csv'):
        data = file
        print(data)

with open('../data/'+data) as f:
    file_Read = f.read()
print(file_Read)
