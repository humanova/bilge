# (c) 2021 Emir Erbasan (humanova)

from datetime import datetime, timedelta

from psycopg2 import *
from peewee import * 
from playhouse.shortcuts import model_to_dict

import bilge
from bilge.logger import logging

config = bilge.config

db = None # BilgeDB instance
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

class Posts(PostBaseModel):
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
    post_id = ForeignKeyField(Posts, backref='sentiment', unique=True)
    positive = DoubleField()
    neutral = DoubleField(null=True)
    negative = DoubleField()
    
class BilgeDB:
    def __init__(self):
        try:
            self.db = bilge_db
            self.db.connect()
            self.init_tables()

        except Exception as e:
            logging.fatal(f"[DB] Couldn't connect to db")
            raise e
    
    def init_tables(self):
        try:
            self.db.create_tables([Sentiment])
        except Exception as e:
            logging.warning("[DB] Couldn't create tables (or tables already exist).")
            logging.warning(e)

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
            logging.warning(f"[DB] Couldn't insert post : {e}")
    
    def add_post_sentiments(self, sentiments): 
        try:
            with self.db.atomic():
                Sentiment.insert_many(sentiments).on_conflict_ignore().execute()
        except Exception as e:
            logging.warning(f"[DB] Couldn't insert sentiments : {e}")
            logging.warning(f"sentiment data : {sentiments}")

    def get_posts_without_sentiment(self, limit:int, before_date=None):
        before_date = datetime.utcnow() - timedelta(hours=1) if before_date is None else before_date
        try:
            posts = (Posts
                     .select(Posts.id, Posts.source, Posts.title, Posts.text, Posts.language)
                     .join(Sentiment, JOIN.LEFT_OUTER)
                     .where((Posts.created_at < before_date) 
                            & (Posts.language.is_null(False)) 
                            & (Posts.language != '') 
                            & (Sentiment.id.is_null()))
                     .limit(limit)
                     )
            return posts
        except Exception as e: 
            logging.warning(f"[DB] Couldn't query the posts without sentiment : {e}")
    
def post_to_dict(post):
    return model_to_dict(post)


db = BilgeDB()