import spacy
from spacy import displacy
from collections import Counter
from pprint import pprint
from collections import defaultdict
import sys
import os
import logging


class NamedEntityRecognition:
    def __init__(self, nlp_model=None):
        if nlp_model is not None:
            self.nlp = nlp_model
        else:
            self.nlp = spacy.load("it_core_news_sm")
        self.LOGGER = self.__get_logger()
        self.LOGGER.info("=" * 120)
        self.LOGGER.info("Named Entity Recognition Ready")

    def named_entity_recognition_process(self, doc):
        # collection.insert_one(self.news_json[0])
        try:

            def add_freq(k, v):
                v["freq"] = freq_dict[k]
                return v

            def escaping(k):
                return k.replace("$", "\\$")

            parsed_doc = self.nlp(doc["parsed_text"])
            freq_dict = defaultdict(int)
            ner_data = {}
            for ent in parsed_doc.ents:
                freq_dict[ent.text.lower()] += 1
                ner_data[ent.text.lower()] = {"label": ent.label_, "freq": None}
            ner_data = {escaping(k): add_freq(k, v) for k, v in ner_data.items()}
            ner_data = self.format_ner_data(ner_data)
            return ner_data
        except Exception:
            exc_type, _, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            # print("{}, {}, {}, {}".format(doc["_id"], exc_type, fname, exc_tb.tb_lineno))
            self.LOGGER.error(
                "{}, {}, {}, {}".format(doc["_id"], exc_type, fname, exc_tb.tb_lineno)
            )

    def format_ner_data(self, ner_data):
        ner_data_fixed = []
        for entity in ner_data.keys():
            new_entry = {}
            new_entry["entity_name"] = entity
            new_entry["label"] = ner_data[entity]["label"]
            new_entry["freq"] = ner_data[entity]["freq"]
            ner_data_fixed.append(new_entry)
        return ner_data_fixed

    def __get_logger(self):
        # create logger
        logger = logging.getLogger("NamedEntityRecognition")
        logger.setLevel(logging.DEBUG)
        # create console handler and set level to debug
        log_path = "core_modules/log/named_entity_recognition.log"
        if not os.path.isdir("core_modules/log/"):
            os.mkdir("core_modules/log/")
        fh = logging.FileHandler(log_path)
        fh.setLevel(logging.DEBUG)
        # create formatter
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        return logger


if __name__ == "__main__":
    named_entity_recognition = NamedEntityRecognition()
    text = "test"
