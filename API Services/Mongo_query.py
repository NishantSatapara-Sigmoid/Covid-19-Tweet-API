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
            return json.JSONEncoder().encode(list(val1))

    def task_2(self,country):
        if (country == "all"):
            exp = [{"$group": {"_id": {"day": "$day", "month": "$month", "country": "$country"}, "count": {"$sum": 1}}}]
            ans = list(self.coll.aggregate(exp))
            return json.JSONEncoder().encode(ans)
        else:
            val2 = []
            country1 = country.split(",")
            for country2 in country1:
                exp = [
                    {'$match':{'country':country2}},
                    {"$group": {"_id": {"day": "$day", "month": "$month", "country": "$country"}, "count": {"$sum": 1},
                                "sourceA": {"$country": "$source"}}},
                    {"$project": {"source": {"$cond": [{"$eq": ["$sourceA", country2]}]}}}

                ]
                all_tasks2 = self.coll.aggregate(exp)
                # val2.append(jsonify(all_tasks2))
                val2 = all_tasks2
            return json.JSONEncoder().encode(list(val2))

    def task_3(self):
        stop_words = ["needn't", 'were', 'ain', 'don', 'did', 'or', 'they', 'will', "she's", "aren't", 'an', 'd', 'weren',
                    'won', "you'll", 'their', 'am', 'having', 'is', 'so', 'once', 'we', "shan't", 'and', 'Corona',
                    'Virus', 'again', 'during', 'other', 'doing', 'now', 'y', 'up', "wasn't", 'themselves', 't', 'few',
                    'some', 'against', 'here', 'there', 'your', "doesn't", 'of', 'isn', 'before', 'are', "didn't",
                    "you're", "haven't", 'myself', 'for',"https", "that'll", 'me', 'until', 'yourself', 'herself', 'those',
                    'because', 'about', 'shouldn', 'that', 'do', 'through', 'such', 'most', 'ourselves', 'above', 'it',
                    'hadn', 'couldn', 'theirs', 'this', 'them', 'a', 'haven', 'ours', 've', 'too', "shouldn't", 'her',
                    'own', "wouldn't", "mustn't", 'been', 'while', 'the', 'whom', 'where', 'between', 'into', 'with',
                    'be', 're', 'by', 'has', 'under', 'only', 'can', 'hers', 'which', "mightn't", 'aren', 'his',
                    "won't", 'i', "don't", 'each', 'in', 'further', 'mustn', 'yourselves', 'what', 'itself', 'not',
                    'more', 'him', 'he', 'to', 'over', 'just', 'yours', 'at', 'being', 'both', 'wasn', 'why', 'as',
                    'who', 'does', 'hasn', "isn't", 'should', 'off', 'then', 'how', 'o', "hadn't", 's', 'll', 'she',
                    "hasn't", 'our', 'wouldn', 'm', 'all', 'was', 'didn', 'mightn', 'my', 'than', 'same', 'ma', 'below',
                    'but', 'down', 'shan', 'had', 'these', 'no', 'any', "you've", "you'd", 'himself', 'you', 'doesn',
                    'from', 'its', 'nor', 'needn', "should've", 'have', 'on', 'when', "weren't", 'if', "couldn't",
                    'out', 'very', "it's", 'after', "co", "de", "I", "n", "", "e", "di", "19", "da", "v", "se", "r",
                    "la", "en", "yg", "1", "2", "k", "n", "k", "a", "l", "3", "ya", "eu", "em", "f", "4", "0", "g"]
        exp = [{"$project": {"tweet": {"$split": ["$tweet", " "]}}},
               {"$unwind": "$tweet"},
               {"$group": {"_id": "$tweet", "total": {"$sum": 1}}},
               {"$sort": {"total": -1}},
               {"$match":
                   {"$and": [
                       {'_id': {'$ne': ""}},
                       {'_id': {"$nin": stop_words}},
                   ]}},
               {"$redact":
                   {"$cond": [
                       {"$gt": [{"$strLenCP": "$_id"}, 2]},
                       "$$KEEP",
                       "$$PRUNE"
                   ]}},
               {"$limit": 100}]
        ans = list(self.coll.aggregate(exp, allowDiskUse=True))
        return json.JSONEncoder().encode(ans)

    def task_4(self,country):
        stop_words = ["https","needn't", 'were', 'ain', 'don', 'did', 'or', 'they', 'will', "she's", "aren't", 'an', 'd',
                      'weren',
                      'won', "you'll", 'their', 'am', 'having', 'is', 'so', 'once', 'we', "shan't", 'and', 'Corona',
                      'Virus', 'again', 'during', 'other', 'doing', 'now', 'y', 'up', "wasn't", 'themselves', 't',
                      'few',
                      'some', 'against', 'here', 'there', 'your', "doesn't", 'of', 'isn', 'before', 'are', "didn't",
                      "you're", "haven't", 'myself', 'for', "that'll", 'me', 'until', 'yourself', 'herself', 'those',
                      'because', 'about', 'shouldn', 'that', 'do', 'through', 'such', 'most', 'ourselves', 'above',
                      'it',
                      'hadn', 'couldn', 'theirs', 'this', 'them', 'a', 'haven', 'ours', 've', 'too', "shouldn't", 'her',
                      'own', "wouldn't", "mustn't", 'been', 'while', 'the', 'whom', 'where', 'between', 'into', 'with',
                      'be', 're', 'by', 'has', 'under', 'only', 'can', 'hers', 'which', "mightn't", 'aren', 'his',
                      "won't", 'i', "don't", 'each', 'in', 'further', 'mustn', 'yourselves', 'what', 'itself', 'not',
                      'more', 'him', 'he', 'to', 'over',"COVID","covid", 'just', 'yours', 'at', 'being', 'both', 'wasn', 'why', 'as',
                      'who', 'does', 'hasn', "isn't", 'should', 'off', 'then', 'how', 'o', "hadn't", 's', 'll', 'she',
                      "hasn't", 'our', 'wouldn', 'm', 'all', 'was', 'didn', 'mightn', 'my', 'than', 'same', 'ma',
                      'below',
                      'but', 'down', 'shan', 'had', 'these', 'no', 'any', "you've", "you'd", 'himself', 'you', 'doesn',
                      'from', 'its', 'nor', 'needn', "should've", 'have', 'on', 'when', "weren't", 'if', "couldn't",
                      'out', 'very', "it's", 'after', "co", "de", "I", "n", "", "e", "di", "19", "da", "v", "se", "r",
                      "la", "en", "yg", "1", "2", "k", "n", "k", "a", "l", "3", "ya", "eu", "em", "f", "4", "0", "g"]

        ans = list(self.coll.aggregate([
            {'$match':{'country':country}},
            {"$project": {"country": "$country", "word": {"$split": ["$tweet", " "]}}},
            {"$unwind": "$word"},
            #{'$project': {'word': {'$toLower': "$word"}, 'country': 1}},
            {'$match': {
                '$and': [
                   # {'word': {'$toLower': "$word"}},
                    {'word': {'$ne': ""}},
                    {'word': {'$nin':stop_words}},
                ]}},
            {"$group": {"_id": {"country": "$country", "word": "$word"}, "total": {"$sum": 1}}},
            {"$sort": {"total": -1}},
            {"$group": {"_id": "$_id.country", "Top_Words": {"$push": {"word": "$_id.word", "total": "$total"}}}},
            {"$project": {"country": 1, "top100Words": {"$slice": ["$Top_Words", 100]}}}
        ], allowDiskUse=True))

        return json.JSONEncoder().encode(ans)
