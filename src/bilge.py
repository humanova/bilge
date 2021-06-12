# (c) 2021 Emir Erbasan (humanova)

import json
from multiprocessing import Pool, Lock
import redis

import database
from sentiment.utils import preprocess
from sentiment.analyzers import TurkishSentimentAnalyzer, EnglishSentimentAnalyzer
from logger import logging

class Bilge:
    def __init__(self, redis_config):
        # initialize redis client, db and the sentiment analyzers
        self.redis_client = redis.Redis(host=redis_config['host'], port=redis_config['port'], db=redis_config['db'])
        self.pubsub = self.redis_client.pubsub()
        self.pubsub_thread = None
        self.db_mutex = Lock()

        self.database = database.BilgeDB()
        self.s_analyzer_tr = TurkishSentimentAnalyzer()
        self.s_analyzer_en = EnglishSentimentAnalyzer()

    def start_listening(self):
        # start listening 'new_posts' pub/sub channel
        self.pubsub.psubscribe(**{'new_posts':self.post_handler})
        self.pubsub_thread = self.pubsub.run_in_thread(sleep_time=0.001)
        logging.info('[Bilge] Started listening "new_posts"')
    
    def stop_listening(self):
        # stop listening 'new_posts' pub/sub channel and the pub/sub thread
        if self.pubsub_thread is not None:
            self.pubsub_thread.stop()
            self.pubsub_thread = None
            self.pubsub.punsubscribe('new_posts')
            logging.info('[Bilge] Stopped listening "new_posts"')

    def post_handler(self, post_message):
        # handle pubsub messages : 
        # unmarshal the posts, calculate the sentiments, update the sentiment table
        self.start_update_missing_sentiments_threads()
        try:
            posts = [self.redis_post_to_model(p) for p in json.loads(post_message['data'])]
            sentiment_data = self.calculate_sentiments(posts)

            self.db_mutex.acquire()
            try:
                self.database.add_post_sentiments(sentiment_data)
            finally:
                self.db_mutex_release()
        except Exception as e:
            logging.warning(f'[Bilge] Could not calculate/insert the sentiments of the posts : {e}')

    def start_update_missing_sentiments_threads(self):
        # handle posts with missing sentiment data
        # query 'limit' number of posts, calculate the sentiments, update the sentiment table
        try:
            posts = self.database.get_posts_without_sentiment(limit=500)
            pool = Pool()
            pool.map(self.update_missing_sentiments, posts)
            pool.close()
            pool.join()
        except Exception as e:
            logging.warning(f'[Bilge] Could not start the update_missing_sentiments thread : {e}')

    def update_missing_sentiments(self, posts):
        sentiment_data = self.calculate_sentiments(posts)
        print(len(sentiment_data))

        self.db_mutex.acquire()
        try:
            self.database.add_post_sentiments(sentiment_data)
        except Exception as e:
            logging.warning(f'[Bilge] Could not calculate/insert the missing sentiments of the posts : {e}')
        finally:
            self.db_mutex.release()

    def calculate_sentiments(self, posts):
        # calculate the sentiments of the posts
        # (except the ones without any meaningful text)
        sentiment_data = []
        for p in posts:
            text = preprocess(p.text).strip()
            if len(text) == 0 and "reddit" in p.source.lower():
                text = p.title

            if len(text) > 0:
                try:
                    analyzer = self.s_analyzer_en if p.language == 'en' else self.s_analyzer_tr
                    sentiment = analyzer.get_sentiment(text)
                    sentiment['post_id'] = p.id
                    sentiment_data.append(sentiment)
                except Exception as e:
                    logging.warning(f'[Bilge] Could not calculate the sentiment : {e}')
                    logging.warning(f'current post data : {p}')
            else:
                continue
        return sentiment_data

    def redis_post_to_model(self, post:dict):
        return database.Posts.create(**{'id': post['ID'],
                                        'created_at' : post['CreatedAt'],
                                        'updated_at': post['UpdatedAt'],
                                        'deleted_at' : post['DeletedAt'],
                                        'title' : post['Title'],
                                        'author' : post['Author'],
                                        'source' : post['Source'],
                                        'text' : post['Text'],
                                        'url' : post['Url'],
                                        'timestamp' : post['Timestamp'],
                                        'score' : post['Score'],
                                        'language' : post['Language']}
                                    )
        