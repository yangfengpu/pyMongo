# -*- coding: utf-8 -*-
'''
@author: feng-pu
'''

from pymongo import MongoClient
client = MongoClient('jpp.no-ip.org', 80)
db = client.educocoMongoLog
collection = db.action_log
distinctSet = collection.distinct("actionName")


for action in distinctSet:
    print action, collection.find({"actionName": action}).count()
    print collection.find({"actionName": action}).distinct("userId") 
    print ""



print collection.distinct("userId")