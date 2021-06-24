# (c) 2021 Emir Erbasan (humanova)

import sys
import traceback

from celery import Celery

import bilge
from bilge import database
from bilge.logger import logging
from bilge.sentiment.analyzers import TurkishSentimentAnalyzer, EnglishSentimentAnalyzer
from bilge.ner.analyzers import EnglishNERAnalyzer
from bilge.sentiment.utils import preprocess

config = bilge.config

app = Celery("bilge", broker=f'redis://{config.redis_host}:{config.redis_port}/{config.redis_db}',
             backend=f'redis://{config.redis_host}:{config.redis_port}/{config.redis_db}')

IN_CELERY_WORKER_PROCESS = sys.argv \
                           and sys.argv[0].endswith('celery') \
                           and 'worker' in sys.argv

# init analyzers if we are in a celery worker
tr_sentiment_analyzer = None
en_sentiment_analyzer = None
en_ner_analyzer = None
tr_ner_analyzer = None

if IN_CELERY_WORKER_PROCESS:
    tr_sentiment_analyzer = TurkishSentimentAnalyzer()
    en_sentiment_analyzer = EnglishSentimentAnalyzer()
    en_ner_analyzer = EnglishNERAnalyzer()
    #tr_ner_analyzer = TurkishSentimentAnalyzer()

# if these sources doesn't contain a proper 'text' then skip them
# for other sources, we will try to use their 'title's
sources_with_inapplicable_titles = ['Twitter', 'Eksisozluk']
sentence_ending_punctuations = ['.', '!', '?']
ner_labels = ['DATE, EVENT, FAC, GPE, LANGUAGE, LAW, LOC, MONEY, NORP, ORG, PERSON, PRODUCT, TIME, WORK_OF_ART']

def fix_sentence_ending(text):
    new_text = text
    new_text += '.' if text[-1] not in sentence_ending_punctuations else ''
    return new_text

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


@app.task
def calculate_and_insert_named_entities(posts):
    # find the named entities mentioned in the posts
    # (except the ones without any meaningful text)
    ner_data = []
    inapplicable_posts = []
    for p in posts:
        # TODO: implement the turkish ner analyzer (research time)
        if p['language'] is None or p['language'] is 'tr':
            continue

        # if 'title' and 'text' are applicable for nlp, then use both by concatenating
        text = preprocess(p['text']).strip()
        if len(text) != 0 and p['source'] not in sources_with_inapplicable_titles:
            text = fix_sentence_ending(text)
            title = preprocess(p['title']).strip()
            if len(title) != 0:
                title = fix_sentence_ending(title)
                text = title + text

        if len(text) > 0:
            try:
                analyzer = en_ner_analyzer if p['language'] == 'en' else tr_ner_analyzer
                entities = analyzer.get_named_entities(text)
                for entity in entities:
                    entity['post_id'] = p['id']
                    ner_data.append(entity)
            except Exception as e:
                logging.warning(f'[Bilge:Tasks] Could not find the named entities : {e}\ncurrent post data : {p}')
                traceback.print_tb(e.__traceback__)
        else:
            inapplicable_posts.append({'post_id': p['id']})
            continue

    # insert to sentiment table, delete from nlp_inapplicable
    if len(ner_data) > 0:
        database.db.add_post_named_entities(ner_data)
        database.db.delete_post_nlpinapplicabilities([p['post_id'] for p in ner_data])

    # insert to nlp_inapplicable post
    if len(inapplicable_posts) > 0:
        database.db.add_post_nlpinapplicabilities(inapplicable_posts)
        database.db.delete_post_named_entities([p['post_id'] for p in inapplicable_posts])