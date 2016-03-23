# -*- coding: utf-8 -*-

from pymongo import MongoClient

class EduDao:
    
    def __init__(self):
        server = "localhost"
        port = 27017
        client = MongoClient(server, port)
        db = client.educocoMongoLog
        self.activity = db.activity_log
        self.action = db.action_log

if __name__ == '__main__':
    mdao = EduDao()