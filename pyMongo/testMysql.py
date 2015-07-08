
import MySQLdb as mysql

mydb = mysql.connect(host="127.0.0.1",
              user="root",
              passwd="",
              db="betaeducoco")
cursor = mydb.cursor()

command = cursor.execute('SELECT DISTINCT(email) FROM `user`')

results = cursor.fetchall()

for result in results:
    print result[0]

mydb.close()
