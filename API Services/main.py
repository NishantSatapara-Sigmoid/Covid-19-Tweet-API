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
    pass
@app.route("/api/task_3")
def task_3():
    pass
@app.route("/api/task_4")
def task_4():
    pass
@app.route("/api/task_5")
def task_5():
    pass
@app.route("/api/task_6")
def task_6():
    pass
@app.route("/api/task_7")
def task_7():
    pass
@app.route("/api/task_8")
def task_8():
    pass
@app.route("/api/task_9")
def task_9():
    pass

if __name__ == "__main__":
    app.run()