# -*- coding: utf-8 -*-
'''
@author: feng-pu
'''
from pymongo import MongoClient

class GuDao:
    
    def __init__(self):
        server = "jpp.no-ip.org"
        port = 80
        client = MongoClient(server, port)
        db = client.educocoMongoLog
        collection = db.gu_log
        

if __name__ == '__main__':
    mdao = GuDao()