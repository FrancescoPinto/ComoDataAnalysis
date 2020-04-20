import csv
'''
import csv
with open("F:\\TwitterDataSet\\Query_3_clean.txt", "w",encoding="utf8") as my_output_file:
    with open("F:\\TwitterDataSet\\Query_3.csv", "r",encoding="utf8") as my_input_file:
        [my_output_file.write(" ".join(row) + '\n') for row in csv.reader(my_input_file)]
    my_output_file.close()'''

import csv

with open("F:\\TwitterDataSet\\Query_3_clean.txt", mode='r',encoding="utf8") as csv_file:
    csv_reader = csv.DictReader(csv_file)
    for row in csv_reader:
        print(row.keys())
        print(f'\t{row["creator"]},{row["hashtags"]}')
