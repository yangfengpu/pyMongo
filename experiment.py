from datetime import datetime, timedelta
from pymongo import MongoClient, ASCENDING, DESCENDING
from bson.objectid import ObjectId


class MongoDao:
    def __init__(self):
        self.client = MongoClient('localhost', 27017)
        self.db = self.client.educocoMongoLog
        self.collection = self.db.activity_log
        self.subject_prefix = "http://educoco.udn.com/vocabulary/user/"

    def query_for_daily_new_users(self, query_date):
        yesterday = query_date - timedelta(days=1)
        tomorrow = query_date + timedelta(days=1)
        users = self.collection.find({"time": {"$gt": yesterday, "$lt": tomorrow}}).distinct("subject")
        return users

    def query_for_period_new_users(self, start_date, end_date):
        users = self.collection.find({"time": {"$gte": start_date, "$lt": end_date}}).distinct("subject")
        return users

    def daily_new_users_count(self, query_date):
        return len(self.query_for_daily_new_users(query_date))

    def query_for_monthly_new_users(self, query_date):
        yesterday = query_date - timedelta(days=30)
        tomorrow = query_date + timedelta(days=1)
        users = self.collection.find({"time": {"$gt": yesterday, "$lt": tomorrow}}).distinct("subject")
        return users

    def monthly_new_users_count(self, query_date):
        return len(self.query_for_monthly_new_users(query_date))

    def query_for_before_new_users(self, query_date):
        tomorrow = query_date + timedelta(days=1)
        users = self.collection.find({"time": {"$lt": tomorrow}}).distinct("subject")
        return users

    def get_user_activity_count(self):
        cursor = self.collection.aggregate([
            {"$group": {"_id": "$subject", "count": {"$sum": 1}}},
            {"$sort": {"count": 1}}
        ])

        for doc in cursor:
            print(doc)

    def get_personal_profile(self, user_id):
        user = self.subject_prefix + str(user_id)
        print(user)
        cursor = self.collection.aggregate([
            {"$match": {"subject": user}},
            {"$group": {"_id": "$predicate", "count": {"$sum": 1}}},
            {"$sort": {"count": 1}}
        ])
        for doc in cursor:
            print(doc)

    def get_personal_profile_by_time(self, user_id, time_start, time_end):
        user = self.subject_prefix + str(user_id)
        print(user)
        cursor = self.collection.aggregate([
            {"$match": {"subject": user, "time": {"$gt": time_start, "$lt": time_end}}},
            {"$group": {"_id": "$predicate", "count": {"$sum": 1}}},
            {"$sort": {"count": 1}}
        ])
        for doc in cursor:
            print(doc)

    def get_active_by_hour(self, time_start, time_end, dayOfWeek):
        cursor = self.collection.aggregate([
            {"$match": {"time": {"$gt": time_start, "$lt": time_end}}},
            {"$match": {"time": {"$gt": time_start, "$lt": time_end}}},
            {"$project": {"hour": {"$hour": "$time"}, "dayOfWeek": { "$dayOfWeek": "$time" }, "subject": "$subject", "subject": {"$concat": ["$subject", { "$substr": [ { "$dayOfYear": "$time" }, 0, 3 ] }]}}},
            #{"$project": {"hour": {"$hour": "$time"}, "dayOfWeek": { "$dayOfWeek": "$time" }, "subject": "$subject"}},
            {"$match": {"dayOfWeek": dayOfWeek}},
            {"$group": {"_id": {"hour": "$hour", "subject": "$subject"}, "count_of_act": {"$sum": 1}}},
            {"$group": {"_id": "$_id.hour", "count": {"$sum": 1}}},
            {"$sort": {"_id": 1}}
        ])
        for doc in cursor:
            print((doc['_id'] + 8 )%24, ",",  doc['count'])#timezone transform
            #print((doc['_id'] + 8 )%24)
            #print (doc['count'])



if __name__ == '__main__':
    md = MongoDao()
    """
    fileArr = []
    start = datetime(2015, 6, 24, 0, 0)
    for i in range(250):
        print(start)
        dau = md.daily_new_users_count(start)
        mau = md.monthly_new_users_count(start)
        fileArr.append([start, dau, mau, 100 * dau/mau])
        start = start + timedelta(days=1)

    import csv
    with open('eggs.csv', 'w', newline='') as csvfile:
        educocowriter = csv.writer(csvfile, delimiter=',',
                            quotechar='|', quoting=csv.QUOTE_MINIMAL)
        for i in range(len(fileArr)):
            educocowriter.writerow(fileArr[i])
    """
    '''
    md.get_user_activity_count()
    md.get_personal_profile(223406)
    md.get_personal_profile(223291)
    md.get_personal_profile(223274)
    '''
    # md.get_personal_profile(212739)
    #md.get_personal_profile_by_time(212739, datetime(2016, 2, 3, 0, 0), datetime(2016, 2, 10, 0, 0))
    md.get_active_by_hour(datetime(2016, 1, 1, 0, 0), datetime(2016, 3, 10, 0, 0), 3)
    #print (len(md.query_for_period_new_users(datetime(2016, 2, 3, 0, 0), datetime(2016, 2, 10, 0, 0))))