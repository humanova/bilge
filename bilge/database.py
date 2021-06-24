# (c) 2021 Emir Erbasan (humanova)

from datetime import datetime, timedelta

from psycopg2 import *
from peewee import *
from playhouse.shortcuts import model_to_dict

import bilge
from bilge.logger import logging

config = bilge.config

db = None  # BilgeDB instance
bilge_db = PostgresqlDatabase(config.db_name,
                              user=config.db_user,
                              password=config.db_password,
                              host=config.db_host,
                              port=config.db_port)


class Posts(Model):
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

    class Meta:
        database = bilge_db
        db_table = 'posts'


class Sentiment(Model):
    post_id = ForeignKeyField(Posts, backref='sentiment', unique=True)
    positive = DoubleField()
    neutral = DoubleField(null=True)
    negative = DoubleField()

    class Meta:
        database = bilge_db
        db_table = 'sentiment'

class NamedEntity(Model):
    post_id = ForeignKeyField(Posts, backref='entity')  # not unique since a post may have multiple named entities
    entity = CharField()
    label = CharField()

    class Meta:
        database = bilge_db
        db_table = 'named_entity'

class NLPInapplicability(Model):
    post_id = ForeignKeyField(Posts, backref='nlp_inapplicability', unique=True)

    class Meta:
        database = bilge_db
        db_table = 'nlp_inapplicability'


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
            self.db.create_tables([Sentiment, NamedEntity, NLPInapplicability])
        except Exception as e:
            logging.warning("[DB] Couldn't create tables (or tables already exist).")
            logging.warning(e)

    # -- sentiment --
    def add_post_sentiment(self, sentiment):
        try:
            with self.db.atomic():
                (Sentiment
                 .insert(**sentiment)
                 .on_conflict(
                    conflict_target=[Sentiment.post_id],
                    preserve=[Sentiment.post_id],
                    update={Sentiment.positive: sentiment['positive'],
                            Sentiment.neutral: sentiment['neutral'],
                            Sentiment.negative: sentiment['negative']})
                 .execute())
        except Exception as e:
            logging.warning(f"[DB] Couldn't insert post : {e}")

    def add_post_sentiments(self, sentiments):
        try:
            with self.db.atomic():
                for s in sentiments:
                    (Sentiment
                     .insert(**s)
                     .on_conflict(
                        conflict_target=[Sentiment.post_id],
                        preserve=[Sentiment.post_id],
                        update={Sentiment.positive: s['positive'],
                                Sentiment.neutral: s['neutral'],
                                Sentiment.negative: s['negative']})
                     .execute())
        except Exception as e:
            logging.warning(f"[DB] Couldn't insert sentiments : {e}")
            logging.warning(f"sentiment data : {sentiments}")

    def delete_post_sentiments(self, post_ids):
        try:
            # posts = NLPInapplicability.select().join().where(NLPInapplicability.post_id << post_ids)
            with self.db.atomic():
                Sentiment.delete().where(Sentiment.post_id.in_(post_ids)).execute()
        except Exception as e:
            logging.warning(f"[DB] Couldn't delete sentiments : {e}")
            logging.warning(f"post ids : {post_ids}")

    # -- named entity --
    def add_post_named_entity(self, named_entity):
        try:
            with self.db.atomic():
                NamedEntity.insert(**named_entity).execute()
        except Exception as e:
            logging.warning(f"[DB] Couldn't insert named entity : {e}")

    def add_post_named_entities(self, named_entities):
        try:
            with self.db.atomic():
                for e in named_entities:
                    NamedEntity.insert(**e).execute()
        except Exception as e:
            logging.warning(f"[DB] Couldn't insert named entities : {e}")
            logging.warning(f"named entity data : {named_entities}")

    def delete_post_named_entities(self, post_ids):
        try:
            with self.db.atomic():
                NamedEntity.delete().where(NamedEntity.post_id.in_(post_ids)).execute()
        except Exception as e:
            logging.warning(f"[DB] Couldn't delete named entities : {e}")
            logging.warning(f"post ids : {post_ids}")

    def get_posts_without_sentiment(self, limit: int, before_date=None):
        before_date = datetime.utcnow() - timedelta(hours=1) if before_date is None else before_date
        try:
            posts = (Posts
                     .select(Posts.id, Posts.source, Posts.title, Posts.text, Posts.language)
                     .join_from(Posts, Sentiment, JOIN.LEFT_OUTER)
                     .join_from(Posts, NLPInapplicability, JOIN.LEFT_OUTER)
                     .where((Posts.created_at < before_date)
                            & (Posts.language.is_null(False))
                            & (Posts.language != '')
                            & (Sentiment.id.is_null())
                            & (NLPInapplicability.id.is_null()))
                     .limit(limit)
                     )
            return posts
        except Exception as e:
            logging.warning(f"[DB] Couldn't query the posts without sentiment : {e}")

    def get_posts_without_named_entity(self, limit: int, before_date=None):
        before_date = datetime.utcnow() - timedelta(hours=1) if before_date is None else before_date
        try:
            posts = (Posts
                     .select(Posts.id, Posts.source, Posts.title, Posts.text, Posts.language)
                     .join_from(Posts, NamedEntity, JOIN.LEFT_OUTER)
                     .join_from(Posts, NLPInapplicability, JOIN.LEFT_OUTER)
                     .where((Posts.created_at < before_date)
                            & (Posts.language == 'en') # temporary (until the TR NER analyzer)
                            & (Posts.language.is_null(False))
                            # & (Posts.language != '') commented out until i implement the turkish ner analyzer
                            & (NamedEntity.id.is_null())
                            & (NLPInapplicability.id.is_null()))
                     .limit(limit)
                     )
            return posts
        except Exception as e:
            logging.warning(f"[DB] Couldn't query the posts without named entities : {e}")

    # -- nlp_inapplicability --
    def add_post_nlpinapplicability(self, post_id):
        try:
            with self.db.atomic():
                NLPInapplicability.insert(
                    post_id=post_id
                ).on_conflict_ignore().execute()
        except Exception as e:
            logging.warning(f"[DB] Couldn't insert inapplicable post id : {e}")

    def add_post_nlpinapplicabilities(self, posts):
        try:
            with self.db.atomic():
                NLPInapplicability.insert_many(posts).on_conflict_ignore().execute()
        except Exception as e:
            logging.warning(f"[DB] Couldn't insert inapplicable post ids : {e}")
            logging.warning(f"posts data : {posts}")

    def delete_post_nlpinapplicabilities(self, post_ids):
        try:
            # posts = NLPInapplicability.select().join().where(NLPInapplicability.post_id << post_ids)
            with self.db.atomic():
                NLPInapplicability.delete().where(NLPInapplicability.post_id.in_(post_ids)).execute()
        except Exception as e:
            logging.warning(f"[DB] Couldn't delete nlp inapplicable posts : {e}")
            logging.warning(f"post ids : {post_ids}")


def post_to_dict(post):
    return model_to_dict(post)


db = BilgeDB()
