# (c) 2021 Emir Erbasan (humanova)

import sys
import traceback 

from celery import Celery

import bilge
from bilge import database
from bilge.logger import logging
from bilge.sentiment.analyzers import TurkishSentimentAnalyzer, EnglishSentimentAnalyzer
from bilge.sentiment.utils import preprocess


config = bilge.config

app = Celery("bilge", broker=f'redis://{config.redis_host}:{config.redis_port}/{config.redis_db}',
                      backend=f'redis://{config.redis_host}:{config.redis_port}/{config.redis_db}')

IN_CELERY_WORKER_PROCESS = sys.argv \
                           and sys.argv[0].endswith('celery')\
                           and 'worker' in sys.argv

# init analyzers if we are in a celery worker
tr_sentiment_analyzer = None
en_sentiment_analyzer = None

if IN_CELERY_WORKER_PROCESS:
    tr_sentiment_analyzer = TurkishSentimentAnalyzer()
    en_sentiment_analyzer = EnglishSentimentAnalyzer()

@app.task
def calculate_and_insert_sentiments(posts):
    # calculate the sentiments of the posts
    # (except the ones without any meaningful text)
    sentiment_data = []
    for p in posts:
        if p['language'] is None:
            continue 

        text = preprocess(p['text']).strip()
        if len(text) == 0 and "reddit" in p['source'].lower():
            text = p['title']

        if len(text) > 0:
            try:
                analyzer = en_sentiment_analyzer if p['language'] == 'en' else tr_sentiment_analyzer
                sentiment = analyzer.get_sentiment(text)
                sentiment['post_id'] = p['id']
                sentiment_data.append(sentiment)
            except Exception as e:
                logging.warning(f'[Bilge/Tasks] Could not calculate the sentiment : {e}\ncurrent post data : {p}')
                traceback.print_tb(e.__traceback__)
        else:
            continue

    if len(sentiment_data) > 0:
        try:
            database.db.add_post_sentiments(sentiment_data)
        except Exception as e:
            logging.warning(f'[Bilge] Could not insert the sentiments of the posts : {e}')
            traceback.print_tb(e.__traceback__)