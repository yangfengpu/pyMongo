import csv
with open('actTea.csv', 'r') as csvfile:
    spamreader = csv.reader(csvfile, delimiter=' ', quotechar='|')
    i = 0
    for row in spamreader:
        print (i, row)
