from celery import Celery
from flask import Flask, request
from flask_sqlalchemy import SQLAlchemy
import logging
import os
import pandas as pd

basedir = os.path.abspath(os.path.dirname(__file__))

app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = 'sqlite:///' + os.path.join(basedir, "database", "app.db")
app.config['CELERY_BROKER_URL'] = "redis://localhost:6379/0"
app.config['CELERY_RESULT_BACKEND'] = 'redis://localhost:6379/0'
app.config['CELERY_ACCEPT_CONTENT'] = ['json']
app.config['CELERY_TASK_SERIALIZER'] = "json"
app.config['CELERY_RESULT_SERIALIZER'] = "json"

celery = Celery(app.import_name, backend=app.config['CELERY_RESULT_BACKEND'], broker=app.config['CELERY_BROKER_URL'])
celery.conf.update(app.config)

db = SQLAlchemy(app)


class Person(db.Model):
    id = db.Column(db.Text, primary_key=True)
    name = db.Column(db.Text, unique=True, nullable=False)
    age = db.Column(db.Text, nullable=True)
    sex = db.Column(db.Text, nullable=True)

    def __repr__(self):
        return '<User %r>' % self.name


db.create_all()


@celery.task(name="tasks.sort")
def sort(df, by, count):
    data = pd.read_json(df, orient="split")
    data.sort_values(by=by, inplace=True)
    data = data.iloc[:count]

    for i in range(data.shape[0]):
        d = data.iloc[i]
        d["Age"] = d["Age"].astype(int)
        person = Person(id=str(d["PassengerId"]), name=d["Name"], age=str(d["Age"]), sex=d["Sex"])
        db.session.add(person)
        db.session.commit()


@app.route("/", methods=["GET"])
@app.route("/index", methods=["GET"])
def index():
    return "Hello World!"


@app.route("/top", methods=["POST", "GET"])
def top():
    out_file = "out.csv"
    try:
        if request.method == "POST":
            data = request.form["data"]
            field = request.form["field"]
            count = int(request.form["count"])

            data = pd.read_json(data, orient="split")
            data.sort_values(by=field, inplace=True)
            data.iloc[:count].to_csv(out_file, index=False)
            return "Result file: " + os.path.join(os.getcwd(), out_file)
        elif request.method == "GET":
            field = request.form["field"]
            count = int(request.form["count"])
            df = pd.read_csv(out_file)

            if count < df.shape[0]:
                df = df.iloc[:count]
            return df[field].to_json(orient="values")
    except Exception as e:
        return "Error: " + str(e)


@app.route("/upload", methods=["POST", ])
def upload():
    try:
        data = request.form["data"]
        field = request.form["field"]
        count = int(request.form["count"])
        logging.info("Params got")

        logging.info("Sorting started")
        sort.delay(data, field, count)
        return "Success!"
    except Exception as e:
        return "Error: " + str(e)


if __name__ == "__main__":
    app.run(debug=True)
