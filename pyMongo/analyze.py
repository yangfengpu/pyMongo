
from pymongo import MongoClient
from bson.son import SON
from datetime import datetime, timedelta
from sets import Set
from types import NoneType
import urllib
password = urllib.quote_plus('12345')

client = MongoClient("localhost")
#client = MongoClient('mongodb://alf:' + '12345' + '@127.0.0.1')
#db = client.educocoMongoLog
db = client.educoco
db.authenticate("alf","12345")
#collection = db.activity_log
collection = db.activity
users = collection.distinct("subject")
#collection.find({"category": "movie"}).distinct("tags");
activities = collection.distinct("predicate")

"""
#aggregate by user

for user in users:
    pipeline = [{"$match" : {"subject" : user}},
            {"$group": {"_id": "$predicate", "count": {"$sum": 1}}}
            ]
    acts = list(db.activity_log.aggregate(pipeline))
    user_id = user.split("/")[-1]
    print "\n", user_id, acts, "\n"
"""


#aggregate by date interval

def aggregateByDateInterval(datePair):
    d1 = datePair[1] #end date
    d2 = datePair[0] #start date

    pipeline = [{"$match" : {"$and": [{"time" : {"$lt": d1 }}, {"time" : {"$gte": d2 }}]}},
            {"$group": {"_id": "$predicate", "count": {"$sum": 1}}}
            ]
    acts = list(collection.aggregate(pipeline))
    for act in acts:
        print act["count"], "    ", act["_id"].split("/")[-1]

def aggregatePersonalDataByDateInterval(datePair, userId):    
    startDatetime = datePair[0] #start date
    endDatetime = datePair[1] #end date
    userUri = "http://educoco.udn.com/vocabulary/user/" + str(userId)
    pipeline = [{"$match" : {"$and": [{"subject": userUri}, {"time" : {"$lt": endDatetime }}, {"time" : {"$gte": startDatetime }}]}},
            {"$group": {"_id": "$predicate", "count": {"$sum": 1}}}
            ]
    acts = list(collection.aggregate(pipeline))
    for act in acts:
        print act["count"], "    ", act["_id"].split("/")[-1]

"""
#aggregate by activity

d1 = datetime(2015, 06, 25, 2) #end date
d2 = datetime(2015, 06, 1, 1) #start date

pipeline = [{"$match" : {"$and": [{"time" : {"$lt": d1 }}, {"time" : {"$gt": d2 }}]}},
            {"$group": {"_id": "$predicate", "count": {"$sum": 1}}}
            ]
acts = list(db.activity_log.aggregate(pipeline))
for act in acts:
    print act["count"], "    ", act["_id"].split("/")[-1]
"""


def aggregateRankingUsersByDateInterval(datePair, predicate):    
    startDatetime = datePair[0] #start date
    endDatetime = datePair[1] #end date
    predicateUri = "http://educoco.udn.com/vocabulary/predicate/" + str(predicate)
    pipeline = [{"$match" : {"$and": [{"predicate": predicateUri}, {"time" : {"$lt": endDatetime }}, {"time" : {"$gte": startDatetime }}]}},
            {"$group": {"_id": "$subject", "count": {"$sum": 1}}},
            { "$sort" : { "count" : -1 } }
            ]
    acts = list(collection.aggregate(pipeline))
    for act in acts:
        print act["count"], "    ", act["_id"].split("/")[-1]
        
def getPredicates():
    return activities

def getUsers():
    return users

def getDistinctObjects():
    objs = collection.distinct("object")
    distinct_objs = Set()
    for s in objs:
        distinct_objs.add(s.split("/")[-2])
    return distinct_objs
    
#explore

def get24HoursDateTime(aDatetime, offset):
    hoursArr = []
    year = aDatetime.year
    month = aDatetime.month
    day = aDatetime.day
    
    for hour in range(24):
        startHour = datetime(year, month, day, hour) + timedelta(hours=offset)
        endHour = startHour + timedelta(hours=1) 
        hoursArr.append((startHour, endHour))
    return hoursArr

def getWeeklyDateTime(aDatetime):
    return aDatetime.weekday()

d1 = datetime(2015,06,30,0)
d2 = datetime(2015,07,21,0)
datePair = (d1, d2)
#aggregatePersonalDataByDateInterval(datePair, 46)#146478
#aggregateByDateInterval(datePair)
#aggregateRankingUsersByDateInterval(datePair, "post-comment")


def getPersonalActivityListByInterval(datePair, userId):    
    startDatetime = datePair[0] #start date
    endDatetime = datePair[1] #end date
    userUri = "http://educoco.udn.com/vocabulary/user/" + str(userId)
    pipeline = [{"$match" : {"$and": [{"subject": userUri}, {"time" : {"$lt": endDatetime }}, {"time" : {"$gte": startDatetime }}]}},
            { "$sort" : { "time" : -1 } }
            ]
    acts = list(collection.aggregate(pipeline))
    for act in acts:
        print act.keys()
        print act["time"], act["predicate"], " "

getPersonalActivityListByInterval(datePair,135378)

#Scenario: Hourly statistics
for hour in get24HoursDateTime(d1, 8):
    print "The Time Interval: ", hour
    #aggregateByDateInterval(hour)
    getPersonalActivityListByInterval(hour,135378) 

print getWeeklyDateTime(d1)
print getWeeklyDateTime(d2)
print collection.find().count()
#distinct IPs
clientIps = db.action.distinct("clientIp")

print clientIps
#distinct IP set
ipset = Set()
nt = 0
for c in clientIps:
    if not (c == None):
        suffix = c.split(".")[-1]
        lengthOfSuffix = len(suffix) + 1
        ipset.add(c[:-(lengthOfSuffix)])
    else:
        nt = nt + 1

print nt, " ", len(ipset)
print ipset

