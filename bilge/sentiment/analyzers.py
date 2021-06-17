# (c) 2021 Emir Erbasan (humanova)

from scipy.special import softmax

from bilge.sentiment.utils import preprocess


class EnglishSentimentAnalyzer:
    def __init__(self):
        """
            Initialize the sentiment analysis model
        """
        from transformers import AutoModelForSequenceClassification, AutoTokenizer
        self.labels = ['negative', 'neutral', 'positive']

        MODEL = "models/cardiffnlp/twitter-roberta-base-sentiment"
        self.tokenizer = AutoTokenizer.from_pretrained(MODEL)
        self.model = AutoModelForSequenceClassification.from_pretrained(MODEL)

        self.tokenizer.save_pretrained(MODEL)
        self.model.save_pretrained(MODEL)

    def get_sentiment(self, text):
        text = preprocess(text)
        sentiments = {}
        output = self.model(**self.tokenizer(text, return_tensors='pt', max_length=512, truncation=True))
        scores = output[0][0].detach().numpy()
        scores = softmax(scores)

        for idx, s in enumerate(scores):
            sentiments[self.labels[idx]] = s

        return sentiments

    def get_sentiments(self, texts):
        return [self.get_sentiment(text) for text in texts]


class TurkishSentimentAnalyzer:
    def __init__(self):
        """
            Initialize the sentiment analysis model
        """
        from transformers import AutoModelForSequenceClassification, AutoTokenizer
        self.labels = ['negative', 'positive']

        MODEL = "models/savasy/bert-base-turkish-sentiment-cased"
        self.tokenizer = AutoTokenizer.from_pretrained(MODEL)
        self.model = AutoModelForSequenceClassification.from_pretrained(MODEL)

        self.tokenizer.save_pretrained(MODEL)
        self.model.save_pretrained(MODEL)

        # self.sentiment_analyzer = pipeline("sentiment-analysis", tokenizer=tokenizer, model=model)

    def get_sentiment(self, text):
        text = preprocess(text)
        sentiments = {}
        output = self.model(**self.tokenizer(text, return_tensors='pt', max_length=512, truncation=True))
        scores = output[0][0].detach().numpy()
        scores = softmax(scores)

        for idx, s in enumerate(scores):
            sentiments[self.labels[idx]] = s
        sentiments['neutral'] = None

        return sentiments

    def get_sentiments(self, texts):
        return [self.get_sentiment(text) for text in texts]
