import time
from celery import Celery
from celery.utils.log import get_task_logger
from sqlalchemy import create_engine
from redis import Redis


from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


redis = Redis(host='redis', port=6379,charset="utf-8", decode_responses=True)
logger = get_task_logger(__name__)

app = Celery('tasks',
             broker='amqp://admin:mypass@rabbit:5672',
             backend='mongodb://mongodb_container:27017/mydb')


db_name = 'database'
db_user = 'username'
db_pass = 'secret'
db_host = 'postgres_container'
db_port = '5432'


db_string = 'postgresql://{}:{}@{}:{}/{}'.format(db_user, db_pass, db_host, db_port, db_name)
db = create_engine(db_string)


DeclarativeBase = declarative_base()


class Post(DeclarativeBase):
    __tablename__ = 'logger2'
    id = Column(Integer, primary_key=True)
    username = Column('username', String)
    message = Column('message', String)


@app.task()
def Send_Message(username, message):
    print(str(username)+" "+str(message))
    try:
        Session = sessionmaker(bind=db)
        session = Session()
        new_mess = Post(username=username, message=message)
        session.add(new_mess)
        session.commit()
    except:
        DeclarativeBase.metadata.create_all(db)
        new_mess = Post(username=username, message=message)
        Session = sessionmaker(bind=db)
        session = Session()
        session.add(new_mess)
        session.commit()
    print("Success")
    #get_message = Get_Message()
    res = "Send message"
    return res


@app.task()
def Get_Message():
    res = []
    Session = sessionmaker(bind=db)
    session = Session()
    for post in session.query(Post):
        print(post)
        res.append({"username": post.username, "message": post.message})
    return res


