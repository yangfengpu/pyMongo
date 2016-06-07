
import MySQLdb as mysql

mydb = mysql.connect(host="127.0.0.1",
              user="root",
              passwd="",
              db="betaeducoco")
cursor = mydb.cursor()

#command = cursor.execute('SELECT DISTINCT(email) FROM `user`')

command = cursor.execute("select firstName, lastName, email from `user` WHERE accessed >= '2015-04-21 15:00:00';")

results = cursor.fetchall()
noneCount = 0
availableEmail = []
for result in results:
    print result[2]
    if (result[2] == None):
        noneCount += 1
    else:
        availableEmail.append(result[2])


print len(results), noneCount, len(availableEmail)
import csv
with open('./eggs.csv', 'wb') as csvfile:
    spamwriter = csv.writer(csvfile, delimiter=' ',
                            quotechar='|', quoting=csv.QUOTE_MINIMAL)
    for email in availableEmail:
        spamwriter.writerow(email)
mydb.close()
