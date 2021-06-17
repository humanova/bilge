from transformers import AutoModelForSequenceClassification, AutoTokenizer

SENTIMENT_EN = "cardiffnlp/twitter-roberta-base-sentiment"
SENTIMENT_TR = "savasy/bert-base-turkish-sentiment-cased"


def dl_pretrained_models(models):
    for m in models:
        tokenizer = AutoTokenizer.from_pretrained(m)
        model = AutoModelForSequenceClassification.from_pretrained(m)

        tokenizer.save_pretrained(m)
        model.save_pretrained(m)


if __name__ == "__main__":
    mls = [SENTIMENT_EN, SENTIMENT_TR]
    print(f"downloading the models : {', '.join(mls)}...")
    dl_pretrained_models(mls)
