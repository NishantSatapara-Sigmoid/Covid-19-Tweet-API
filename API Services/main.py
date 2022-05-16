from flask import Flask,request , redirect,jsonify
from Mongo_query import Mongo_query
app = Flask(__name__)

Mongo_Object = Mongo_query()
@app.route('/')
def home():
    return "Start Mock_Project"

@app.route("/api/task_1")
def task_1():
    if 'country' in request.args:
        country = str(request.args['country'])
        return Mongo_Object.task_1(country)
    else:
        return Mongo_Object.task_1('all')

@app.route("/api/task_2")
def task_2():
    if 'country' in request.args:
        country = str(request.args['country'])
        return Mongo_Object.task_2(country)
    else:
        return Mongo_Object.task_2('all')

@app.route("/api/task_3")
def task_3():
    return Mongo_Object.task_3()

@app.route("/api/task_4")
def task_4():
    if 'country' in request.args:
        country=str(request.args['country'])
    else:
        country="India"
    return Mongo_Object.task_4(country)


if __name__ == "__main__":
    app.run(debug=False)
