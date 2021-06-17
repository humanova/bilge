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

# if these sources doesn't contain a proper 'text' then skip them
# for other sources, we will try to use their 'title's
sources_with_inapplicable_titles = ['Twitter', 'Eksisozluk']

@app.task
def calculate_and_insert_sentiments(posts):
    # calculate the sentiments of the posts
    # (except the ones without any meaningful text)
    sentiment_data = []
    inapplicable_posts = []
    for p in posts:
        if p['language'] is None:
            continue 

        text = preprocess(p['text']).strip()
        # try using the post 'title' instead of 'text'
        if len(text) == 0 and p['source'] not in sources_with_inapplicable_titles:
            text = preprocess(p['title']).strip()

        if len(text) > 0:
            try:
                analyzer = en_sentiment_analyzer if p['language'] == 'en' else tr_sentiment_analyzer
                sentiment = analyzer.get_sentiment(text)
                sentiment['post_id'] = p['id']
                sentiment_data.append(sentiment)
            except Exception as e:
                logging.warning(f'[Bilge:Tasks] Could not calculate the sentiment : {e}\ncurrent post data : {p}')
                traceback.print_tb(e.__traceback__)
        else:
            inapplicable_posts.append({'post_id': p['id']})
            continue
    
    # insert to sentiment table, delete from nlp_inapplicable
    if len(sentiment_data) > 0:
        database.db.add_post_sentiments(sentiment_data)
        database.db.delete_post_nlpinapplicabilities([p['post_id'] for p in sentiment_data])

    # insert to nlp_inapplicable post
    if len(inapplicable_posts) > 0:
        database.db.add_post_nlpinapplicabilities(inapplicable_posts)
        database.db.delete_post_sentiments([p['post_id'] for p in inapplicable_posts])