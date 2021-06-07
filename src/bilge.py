# (c) 2021 Emir Erbasan (humanova)

import redis
import json

import database
from sentiment.analyzers import TurkishSentimentAnalyzer, EnglishSentimentAnalyzer

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

        self.pubsub_thread = None
    
    def start_listening(self):
        """
            start listening 'new_posts' pub/sub channel
        """
        self.pubsub.psubscribe(**{'new_posts':self.post_handler})
        self.pubsub_thread = self.pubsub.run_in_thread(sleep_time=0.001)
        print('[Bilge] Started listening "new_posts"')

    def stop_listening(self):
        if self.pubsub_thread is not None:
            self.pubsub_thread.stop()
            self.pubsub_thread = None
            self.pubsub.punsubscribe('new_posts')
            print('[Bilge] Stopped listening "new_posts"')

    def post_handler(self, post_message):
        """
            post_message['data'] contains a json array of posts published by the scraper
        """
        posts = json.loads(post_message['data'])
        sentiment_data = []
        # calculate the sentiments of the posts
        for p in posts:
            text = sentiment.utils.preprocess(['Text']).strip()

            if len(text) == 0 and "reddit" in p['Source'].lower():
                text = p['Title']
            elif len(text) > 0:
                try:
                    analyzer = self.s_analyzer_en if p['Language'] == 'en' else self.s_analyzer_tr
                    sentiment = analyzer.get_sentiment(p['Text'])
                    sentiment['post_id'] = p['ID']
                    sentiment_data.append(sentiment)
                except Exception as e:
                    print(f'[Bilge] Exception in post_handler() : {e}')
                    print(f'current post data : {p}')
            else:
                continue
        # insert posts to sentiment table
        try:
            self.database.add_post_sentiments(sentiment_data)
        except Exception as e:
            print(f"[Bilge] Exception in post_handler() : {e}")
            print(f'sentiment_data list : {sentiment_data}')