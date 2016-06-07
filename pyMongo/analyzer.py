from pymongo import MongoClient
from datetime import datetime, timedelta

class BaseMongoAnalyzer:
    def __init__(self, collection):
        self.collection = collection
        self.pipeline = []
    
    def get24HoursDateTime(self, aDatetime, offset):
        hoursArr = []
        year = aDatetime.year
        month = aDatetime.month
        day = aDatetime.day
        for hour in range(24):
            startHour = datetime(year, month, day, hour) + timedelta(hours=offset)
            endHour = startHour + timedelta(hours=1) 
            hoursArr.append((startHour, endHour))
        return hoursArr    
    
    def getWeeklyDateTime(self, aDatetime):
        return aDatetime.weekday()
    
    def getIntervalConditions(self, timeLabel, startTime, endTime):
        conditions = [{timeLabel : {"$lt": endTime }}, {timeLabel : {"$gte": startTime }}]
        return conditions
    
    def getFieldMatchCondition(self, fieldLabel, value):
        condition = {fieldLabel : value }
        return condition
    

    def appendCondition(self, original, append_conditions):
        if (type(append_conditions) is list):
            for condition in append_conditions:
                self.appendCondition(condition)   
        if (type(append_conditions) is str): #single condition
            original.append(append_conditions)
        else:
            pass #if the condition is not string, skip it

class Conditions:
        
    def __init__(self, timeLabel, conditions = []):
        if (type(conditions) is list):
            self.conditions = conditions
            self.startTime = datetime.now()
            self.endTime = datetime.now()
            self.timeLabel = timeLabel
        else:
            self.conditions = []  
            print "The conditions is not a list, creating an empty list by default!"
    def getCurrentConditions(self):
        return self.conditions
    def setTimeInterval(self, startTime, endTime):
        self.startTime = startTime
        self.endTime = endTime
    def __setIntervalConditions(self, timeLabel, startTime, endTime):
        conditions = [{timeLabel : {"$lt": endTime }}, {timeLabel : {"$gte": startTime }}]
        return conditions
    def conduct(self, conditions):
        time_conditions = self.__setIntervalConditions(self, self.timeLabel, startTime, endTime)
        andMerge = {"$match" : {"$and": []}}
        andMerge["$match"]["$and"] = self.conditions.append(time_conditions[0], time_conditions[1])
        self.pipeline.append(andMerge)
        print self.pipeline
        query_result = list(self.collection.aggregate(self.pipeline))
        return query_result
    def append(self, new_conditions):
        if (type(new_conditions) is list):
            for condition in new_conditions:
                self.append(condition)
        if(type(new_conditions) is str):
            self.conditions.append(new_conditions)
        return self # a builder
    def resetAs(self, reset):
        self.conditions = reset
    def toList(self):
        return self.conditions

class DefaultAnalyzer(BaseMongoAnalyzer):
    pass
    
    
if __name__ == '__main__':
    client = MongoClient("localhost")
    #db = client.educocoMongoLog
    db = client.educoco
    #collection = db.activity_log
    collection = db.activity
    da = DefaultAnalyzer(collection)
    endTime = datetime(2015, 06, 25, 2) #end date
    startTime = datetime(2015, 06, 1, 1) #start date
    
    #cos = da.andMergeConditions(da.getIntervalConditions("time", startTime, endTime))

        
    
