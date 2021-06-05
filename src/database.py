# (c) 2021 Emir Erbasan (humanova)

from datetime import timezone
from psycopg2 import *
from peewee import * 
import logging
import confparser
#logging.basicConfig(level=logging.DEBUG)

config = confparser.get("../config.json")

bilge_db = PostgresqlDatabase(config.db_name,
                              user=config.db_user,
                              password=config.db_password,
                              host=config.db_host,
                              port=config.db_port)


class PostBaseModel(Model):
    class Meta:
        database = bilge_db
        db_table = 'posts'

class SentimentBaseModel(Model):
    class Meta:
        database = bilge_db
        db_table = 'sentiment'

class Post(PostBaseModel):
    created_at = DateTimeField()
    updated_at = DateTimeField()
    deleted_at = DateTimeField()
    title = CharField()
    author = CharField()
    source = CharField()
    text = CharField()
    url = CharField(unique=True)
    timestamp = IntegerField()
    score = IntegerField()
    language = CharField()

class Sentiment(SentimentBaseModel):
    post_id = ForeignKeyField(Post, backref='sentiment')
    positive = DoubleField()
    neutral = DoubleField()
    negative = DoubleField()
    
class BilgeDB:
    def __init__(self):
        try:
            self.db = bilge_db
            self.db.connect()
            self.init_tables()

        except Exception as e:
            print(f"[Bilge:DB] Couldn't connect to db : {e}")
    
    def init_tables(self):
        try:
            self.db.create_tables([Sentiment])
        except Exception as e:
            print("[Bilge:DB] Couldn't create tables (or tables already exist).")
            print(e)

    def add_post_sentiment(self, post_id, positive, neutral, negative):
        try: 
           with self.db.atomic():
                post = self.Sentiment.create(
                    post_id= post_id,
                    positive= positive,
                    neutral= neutral,
                    negative= negative
                )
                return post
        except Exception as e:
            print(f"[Bilge:DB] Couldn't insert post : {e}")
    
    def add_post_sentiments(self, sentiments): 
        try:
            Sentiment.insert_many(sentiments).on_conflict_ignore().execute()
        except Exception as e:
            print(f"[Bilge:DB] Couldn't insert posts : {e}")