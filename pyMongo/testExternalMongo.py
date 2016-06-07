from pymongo import MongoClient
import datetime
#client = MongoClient("10.206.239.3", 80 )
#client = MongoClient("jpp.no-ip.org", 80)
client = MongoClient("localhost")
db = client.educocoMongoLog

collection = db.activity_log

#print collection.find({"userId": 24493}).count()

acts = collection.find({"subject": "http://educoco.udn.com/vocabulary/user/146478"})
print acts.count()

"""
users = collection.distinct("subject")
print users

for user in users:
    u_record = collection.find({"subject": user})
    for u in u_record:
        print "       ", u["predicate"].split("/")[-1], u["time"]
    print user, u_record.count(), u_record
"""
#user27003 = collection.find({"userId": 27003})

#for record in user27003:
#    print record.get("actionName")

#print collection.distinct("userId")


"""
a = datetime.datetime.now()
for k in range(10):
    collection.insert_one({"name" : "small_bai", "date" : datetime.datetime.utcnow(), "tags": [1,2,3]})
b = datetime.datetime.now()
print "done, takes ", (b-a) 
"""


