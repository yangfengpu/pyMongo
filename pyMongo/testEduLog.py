
import requests
import datetime


def getPayload():
    today = datetime.date.today()
    payload = {'object': today, 'predicate': 'http://www.google.com', 'subject': 'site', 'context': '{"test": 123, "small-bai" : "www"}'}
    return payload


for r in range(1000):
    r = requests.post("http://localhost:8080/educoco/activityLogger", data=getPayload())
    print(r.text)