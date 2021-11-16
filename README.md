# bilge

Bilge is NLP analysis service, specifically built to do NER and Sentiment analysis on posts scraped from [mergen](https://github.com/humanova/mergen)).

Using Redis pub/sub channels to recieve posts, PostgreSQL to store the analysis results. Redis is also used as a Message Broker to schedule NLP tasks for bilge workers to process. 

Altough easily modifiable, bilge has minimal 'data cleaning' features. This service was built according to my OSINT projects needs.
