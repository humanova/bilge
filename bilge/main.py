# (c) 2021 Emir Erbasan (humanova)
#  Bilge : NLP analysis for mergen posts

import traceback
import json

import redis

from bilge import database
from bilge.logger import logging
from bilge.tasks import calculate_and_insert_sentiments


class Bilge:
    def __init__(self, redis_client):
        # initialize redis client, db and the sentiment analyzers
        self.redis_client = redis_client
        self.pubsub = self.redis_client.pubsub()
        self.pubsub_thread = None

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
        self.update_missing_sentiments()
        try:
            posts = [self.redis_post_to_model_dict(p) for p in json.loads(post_message['data'])]
            calculate_and_insert_sentiments.delay(posts)
        except Exception as e:
            logging.warning(f'[Bilge] Could not calculate/insert the sentiments of the posts : {e}')
            traceback.print_tb(e.__traceback__)

    def update_missing_sentiments(self):
        # handle posts with missing sentiment data
        # query 'limit' number of posts, calculate the sentiments, update the sentiment table
        try:
            # 500 posts (as dict) in 10 slices
            posts = [database.post_to_dict(p) for p in database.db.get_posts_without_sentiment(limit=500)]
            post_slices = [posts[x:x + 50] for x in range(0, len(posts), 50)]

            for slice in post_slices:
                calculate_and_insert_sentiments.delay(slice)
        except Exception as e:
            logging.warning(f'[Bilge] Could not send missing sentiments to the worker : {e}')
            traceback.print_tb(e.__traceback__)

    def redis_post_to_model_dict(self, post:dict):
        return {'id': post['ID'],
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


if __name__ == "__main__":
    import bilge
    config = bilge.config

    try:
        redis_client = redis.Redis(host= config.redis_host, port=config.redis_port, db=config.redis_db)
        bilge = Bilge(redis_client)
        bilge.start_listening()
    except Exception as e:
        traceback.print_tb(e.__traceback__)
        logging.fatal(f"[Bilge] Couldn't initialize bilge : {e}")
        quit()

