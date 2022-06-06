import json
import pika
import flask
from flask import request, jsonify
import boto3
from celery import Celery
import pymongo
from redis import Redis

app = flask.Flask(__name__)


simple_app = Celery('broker',
                    broker='amqp://admin:mypass@rabbit:5672',
                    backend='mongodb://mongodb_container:27017/mydb')


@app.route('/sendMessage', methods=['POST'])
def sendMessage():
    data = json.loads(request.data)
    username = data.get('username')
    message = data.get('message')
    task = simple_app.send_task('tasks.Send_Message', kwargs={'username': username, 'message': message})
    app.logger.info(f"ID: {task.id}")
    #Async
    result = simple_app.AsyncResult(str(task.id))
    app.logger.info(f"result: {result}")
    #result = result.result

    return jsonify({"info": "success"})


@app.route('/getMessage', methods=['GET'])
def getChat():
    app.logger.info("Invoking Method ")
    task = simple_app.send_task('tasks.Get_Message')
    app.logger.info(f"ID: {task.id}")
    result = simple_app.AsyncResult(str(task.id))
    app.logger.info(f"result: {result}")
    result = result.result

    return jsonify({"info": "success", "result": str(result)})


