import spacy


class EnglishNERAnalyzer:
    def __init__(self):
        """
            Initialize the NER model
        """
        self.model = spacy.load("en_core_web_trf")

    def get_named_entities(self, text):
        doc = self.model(text)

        entities = []
        for entity in doc.ents:
            entities.append({"entity": entity, "label": entity.label_})

        return entities
