# (c) 2021 Emir Erbasan (humanova)

import redis
import json

import database
from sentiment.utils import preprocess
from sentiment.analyzers import TurkishSentimentAnalyzer, EnglishSentimentAnalyzer
from logger import logging

class Bilge:
    def __init__(self, redis_config):
        """
            initialize redis client, db and the sentiment analyzers
        """
        self.redis_client = redis.Redis(host=redis_config['host'], port=redis_config['port'], db=redis_config['db'])
        self.pubsub = self.redis_client.pubsub()

        self.database = database.BilgeDB()
        self.s_analyzer_tr = TurkishSentimentAnalyzer()
        self.s_analyzer_en = EnglishSentimentAnalyzer()
    
    def start_listening(self):
        """
            start listening 'new_posts' pub/sub channel
        """
        self.pubsub.psubscribe(**{'new_posts':self.post_handler})
        self.pubsub_thread = self.pubsub.run_in_thread(sleep_time=0.001)
        logging.info('[Bilge] Started listening "new_posts"')

    def stop_listening(self):
        if self.pubsub_thread is not None:
            self.pubsub_thread.stop()
            self.pubsub_thread = None
            self.pubsub.punsubscribe('new_posts')
            logging.info('[Bilge] Stopped listening "new_posts"')

    def post_handler(self, post_message):
        """
            post_message['data'] contains a json array of posts published by the scraper
        """
        posts = json.loads(post_message['data'])
        sentiment_data = []
        # calculate the sentiments of the posts
        # (except the ones without any meaningful text)
        for p in posts:
            text = preprocess(p['Text']).strip()

            if len(text) == 0 and "reddit" in p['Source'].lower():
                text = p['Title']
            elif len(text) > 0:
                try:
                    analyzer = self.s_analyzer_en if p['Language'] == 'en' else self.s_analyzer_tr
                    sentiment = analyzer.get_sentiment(p['Text'])
                    sentiment['post_id'] = p['ID']
                    sentiment_data.append(sentiment)
                except Exception as e:
                    logging.warning(f'[Bilge] Exception in post_handler() : {e}')
                    logging.warning(f'current post data : {p}')
            else:
                continue
        # insert posts to sentiment table
        try:
            self.database.add_post_sentiments(sentiment_data)
        except Exception as e:
            logging.warning(f'[Bilge] Exception in post_handler() : {e}')
            logging.warning(f'sentiment_data list : {sentiment_data}')