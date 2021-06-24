from transformers import AutoModelForSequenceClassification, AutoTokenizer
import subprocess
import sys

SENTIMENT_EN = "cardiffnlp/twitter-roberta-base-sentiment"
SENTIMENT_TR = "savasy/bert-base-turkish-sentiment-cased"
NER_EN = "en_core_web_trf"

def dl_pretrained_hf_models(models):
    for m in models:
        tokenizer = AutoTokenizer.from_pretrained(m)
        model = AutoModelForSequenceClassification.from_pretrained(m)

        tokenizer.save_pretrained(m)
        model.save_pretrained(m)

def dl_pretrained_spacy_models(models):
    for m in models:
        subprocess.check_call([sys.executable, "-m", "spacy", "download", m])

if __name__ == "__main__":
    hf_mls = [SENTIMENT_EN, SENTIMENT_TR]
    sp_mls = [NER_EN]

    print(f"downloading huggingface models : {', '.join(hf_mls)}...")
    dl_pretrained_hf_models(hf_mls)

    print(f"downloading spacy models : {', '.join(hf_mls)}...")
    dl_pretrained_spacy_models(sp_mls)
