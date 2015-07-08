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
    
    def andMergeConditions(self, conditions):
        andMerge = {"$match" : {"$and": []}}
        andMerge["$match"]["$and"] = conditions
        self.pipeline.append(andMerge)
        
    
    
    
