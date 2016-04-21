from experiment import MongoDao
from datetime import datetime, timedelta
if __name__ == '__main__':
    md = MongoDao()
    dateX1 = datetime(2015, 11, 16, 0, 0)
    x1_users = set(md.query_for_daily_new_users(dateX1))
    print (len(x1_users))

    dateX2 = datetime(2015, 11, 17, 0, 0)
    x2_users = set(md.query_for_daily_new_users(dateX2))
    print(len(x2_users))
    x1_x2 = x1_users.intersection(x2_users)
    print(len(x1_x2))

    start = datetime(2015, 11, 18, 0, 0) + + timedelta(days=90)
    for i in range(30):
        n_x1_x2 = x1_x2.intersection(set(md.query_for_daily_new_users(start)))
        print(len(n_x1_x2))
        start = start + timedelta(days=1)

