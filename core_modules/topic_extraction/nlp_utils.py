import pandas as pd

import spacy
import json
import logging
import os
import sys

from itertools import chain


# Utility functions to parse text
class NLPUtils:
    def __init__(self, lang="en"):
        self.lang = lang
        self.doc = None
        if self.lang == "es":
            self.nlp = spacy.load("es_core_news_md")
        elif self.lang == "de":
            self.nlp = spacy.load("de_core_news_md")
        elif self.lang == "fr":
            self.nlp = spacy.load("fr_core_news_md")
        elif self.lang == "it":
            self.nlp = spacy.load("it_core_news_md")
        elif self.lang == "nl":
            self.nlp = spacy.load("nl_core_news_md")
        else:
            # English is default
            self.nlp = spacy.load("en_core_web_md")
        self.fix_stop_words()
        self.load_custom_stop_words()
        self.LOGGER = self.__get_logger()
        self.LOGGER.info("=" * 120)
        self.LOGGER.info("NLP Utils ready")

    def parse_text(self, raw_data):
        """
        General function to parse a set of texts
        """
        # Check parsing before this point (should be good with pymongo)
        try:
            # Build spaCy's doc object
            self.doc = self.nlp(raw_data["text"])
            # Retrieve sentences
            sentences = self.sentence_tokenize(self.doc)
            # print(len(sentences))
            # Lemmatize + remove stop words
            lemmas = self.lemmatize_tokens(sentences)
            # print(len(lemmas))
            # Flatten results into a single list
            parsed_text = self.flatten_list(lemmas)

            return parsed_text
        except Exception:
            exc_type, _, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            # print("{}, {}, {}, {}".format(doc["_id"], exc_type, fname, exc_tb.tb_lineno))
            self.LOGGER.error(
                "{}, {}, {}, {}".format(raw_data["_id"], exc_type, fname, exc_tb.tb_lineno)
            )

    def fix_stop_words(self):
        """
        Despite being present in spaCy's models, sometimes stop words aren't picked up.
        This workaround forces them to be removed.
        """
        for word in self.nlp.Defaults.stop_words:
            self.nlp.vocab[word].is_stop = True
        if self.lang == "it":
            self.nlp.vocab["dio"].is_stop = True
        elif self.lang == "de":
            self.nlp.vocab["Prozent"].is_stop = True
        return

    def load_custom_stop_words(self):
        with open("core_modules/topic_extraction/custom_stop_words.json", "r") as sw:
            custom_stop_words = json.load(sw)["s_words"]
        # custom_stop_words = custom_stop_words['s_words']
        self.add_custom_stop_words(custom_stop_words)

    def add_custom_stop_words(self, custom_stop_words):
        """
        If any, custom words are added by flagging them in the language model (nlp)
        """
        for cw in custom_stop_words:
            self.nlp.vocab[cw].is_stop = True
        return

    def sentence_tokenize(self, data):
        """
        Creates a list of strings with each one being a sentence from that document.
        """
        return [sent for sent in data.sents]

    def lemmatize_tokens(self, data):
        """
        Lemmatizing each word + remove stop words in each sentence
        """
        lemmas = []
        for sent in data:
            sent_tokens = []
            for token in sent:
                candidate = token.lemma_.replace("’", "")
                if (
                    not self.nlp.vocab[candidate].is_stop
                    and not token.is_punct
                    and len(candidate) > 1
                    and not candidate.isspace()
                ):
                    sent_tokens.append(candidate)
            lemmas.append(sent_tokens)
            sent_tokens = []
        return lemmas

    def flatten_list(self, data):
        """
        Flattens the lemmatized sentences into a single list,
        ready for gensim's LDA implementation
        """
        return list(chain.from_iterable(data))

    def __get_logger(self):
        # create logger
        logger = logging.getLogger("NLPUtils")
        logger.setLevel(logging.DEBUG)
        # create console handler and set level to debug
        log_path = "core_modules/log/nlp_utils.log"
        if not os.path.isdir("core_modules/log"):
            os.mkdir("core_modules/log")
        fh = logging.FileHandler(log_path)
        fh.setLevel(logging.DEBUG)
        # create formatter
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        return logger


if __name__ == "__main__":
    nlp_utils = NLPUtils()
