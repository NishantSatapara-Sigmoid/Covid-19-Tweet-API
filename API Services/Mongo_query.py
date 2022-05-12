from pymongo import MongoClient
import json

host = "mongodb://localhost:27017"
class Mongo_query:
    def __init__(self):
        self.client = MongoClient(host)
        self.db = self.client.TwitterDB
        self.coll = self.db.Tweets

    def task_1(self,country):
        if (country == "all"):
            aggr = [{"$group":{ "_id" : "$country", "count":{"$sum" : 1}}},{"$sort" : { "count": -1} }]
            ans = list( self.coll.aggregate(aggr))
            return json.JSONEncoder().encode(ans)
        else:
            country1 = country.split(",")
            val1 = []
            for country2 in country1:
                agr = self.coll.count_documents({"country": country2})
                val1.append({"country": country2, "count": agr})
            return json.JSONEncoder().encode(val1)

    def task_2(self,country):
        if (country == "all"):
            exp = [{"$group": {"_id": {"day": "$day", "month": "$month", "country": "$country"}, "count": {"$sum": 1}}}]
            all_tasks2 = self.coll.aggregate(exp)
            return all_tasks2
        else:
            pass




