import pymongo
from pymongo import MongoClient
from pymongo.errors import CursorNotFound, ServerSelectionTimeoutError
import os
import time
import logging
import yaml
import sys
import gc
import pprint
import datetime
from collections import Counter
from dateutil.relativedelta import relativedelta
import re


class MostFrequentWords:
    def __init__(self):
        # init
        with open("../configuration/configuration.yaml") as f:
            self.CONFIG = yaml.load(f, Loader=yaml.FullLoader)
        self.LOGGER = self.__get_logger()
        # mongourl = "mongodb://admin:adminpassword@localhost:27017"
        mongourl = self.CONFIG["mongourl"]
        self.MONGO_CLIENT = MongoClient(mongourl)
        self.N_MOST_FREQUENT = 100
        self.QUERY = {}

    def set_query(self, start_date):
        end_date = start_date + relativedelta(months=1) - datetime.timedelta(days=1)
        self.QUERY = {
            "discoverDate": {"$gte": start_date, "$lte": end_date},
        }

    def db_news_extraction(self, lang, start_date):
        self.set_query(start_date)
        if lang != "it":
            name_coll = "article_" + lang
        else:
            name_coll = "article"
        collection = self.MONGO_CLIENT["news"][name_coll]
        not_processed_docs = collection.find(self.QUERY, no_cursor_timeout=True)
        return collection, not_processed_docs

    def update_db(self, lang, most_frequent_words, end_date):
        collection = self.MONGO_CLIENT["statistics"]["month_" + lang]
        start_date = end_date.replace(day=1)
        end_ts = int(end_date.timestamp())
        date_range = "{}-{}".format(str(start_date), str(end_date))
        try:
            collection.insert_one(
                {
                    "ts": end_ts,
                    "dateRange": date_range,
                    "most_frequent_words": most_frequent_words,
                }
            )
            return 1
        except Exception as e:
            exc_type, _, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            # print("{}, {}, {}, {}".format(doc["_id"], exc_type, fname, exc_tb.tb_lineno))
            self.LOGGER.error(
                "{}, {}, {}, {}, {}".format(
                    "month_" + lang, exc_type, fname, exc_tb.tb_lineno, str(e)
                )
            )
            return 0

    def check_start_condition(self, lang):
        last_ts = self.CONFIG["most_frequent_word_extraction"]["last_ts_" + lang]
        datetime_from_ts = datetime.datetime.fromtimestamp(last_ts)
        today = datetime.datetime.now()
        days_range = today - datetime_from_ts
        if (
            datetime_from_ts is None
            or days_range.days >= self.CONFIG["most_frequent_word_extraction"]["days_range"]
        ):
            return True
        return False

    def check_stop_condition(self, lang, end_date):
        today = datetime.datetime.now()
        days_range = today - end_date
        if days_range.days >= self.CONFIG["most_frequent_word_extraction"]["days_range"]:
            return False
        return True

    def clean_text(self, text):
        text = (
            text.lower()
            .replace("stare", "")
            .replace("dio", "")
            .replace(".", "")
            .replace("'", "")
            .replace(",", "")
        )
        text = re.sub("(\s+)(a|an|and|the)(\s+)", " ", text)
        text = re.sub(
            "(\s+)(il|lo|la|il|gli|le|di|a|da|in|con|su|per|tra|fra|l)(\s+)", " ", text
        )
        return text

    def main(self):
        self.LOGGER.info("=" * 120)
        self.LOGGER.info("STARTED EXTRACTION OF MOST FREQUENT WORDS")
        for lang in self.CONFIG["collections_lang"]:
            self.LOGGER.info("CURRENT COLLECTION: ARTICLE {}".format(lang.upper()))
            has_to_start = self.check_start_condition(lang)
            if has_to_start:
                self.LOGGER.info("Extracting news from db...")
                stop = False
                last_ts = self.CONFIG["most_frequent_word_extraction"]["last_ts_" + lang]
                datetime_from_ts = datetime.datetime.fromtimestamp(last_ts)
                start_date = datetime_from_ts + datetime.timedelta(days=1)
                end_date = start_date + relativedelta(months=1) - datetime.timedelta(days=1)
                while not stop:
                    collection, not_processed_docs = self.db_news_extraction(lang, start_date)
                    not_processed_docs_count = collection.count_documents(self.QUERY)
                    self.LOGGER.info(
                        "{} Articles to analyze in date range {} - {} ...".format(
                            not_processed_docs_count, start_date, end_date
                        )
                    )
                    counter = Counter()
                    if not_processed_docs_count == 0:
                        ## next month
                        stop = self.check_stop_condition(lang, end_date)
                        end_ts = int(end_date.timestamp())
                        self.CONFIG["most_frequent_word_extraction"][
                            "last_ts_" + lang
                        ] = end_ts
                        with open("../configuration/configuration.yaml", "w") as f:
                            yaml.dump(self.CONFIG, f)
                        datetime_from_ts = datetime.datetime.fromtimestamp(end_ts)
                        start_date = datetime_from_ts + datetime.timedelta(days=1)
                        end_date = (
                            start_date + relativedelta(months=1) - datetime.timedelta(days=1)
                        )
                    else:
                        try:
                            for doc in not_processed_docs:
                                if "parsedText" in doc:
                                    doc_text = doc["parsedText"]
                                    doc_text = self.clean_text(doc_text)
                                    counter.update(doc_text.split())
                            most_frequents = dict(counter.most_common(self.N_MOST_FREQUENT))
                            res = self.update_db(lang, most_frequents, end_date)
                            if res:
                                # update to db is ok, updating also new start and end date
                                end_ts = int(end_date.timestamp())
                                self.CONFIG["most_frequent_word_extraction"][
                                    "last_ts_" + lang
                                ] = end_ts
                                with open("../configuration/configuration.yaml", "w") as f:
                                    yaml.dump(self.CONFIG, f)
                                datetime_from_ts = datetime.datetime.fromtimestamp(end_ts)
                                start_date = datetime_from_ts + datetime.timedelta(days=1)
                                end_date = (
                                    start_date
                                    + relativedelta(months=1)
                                    - datetime.timedelta(days=1)
                                )
                            stop = self.check_stop_condition(lang, end_date)
                        except (CursorNotFound, ServerSelectionTimeoutError):
                            self.LOGGER.error("Lost cursor. Retry...")
                            not_processed_docs.close()
                self.LOGGER.info("COLLECTION WITH LANG {} ANALYZED".format(lang))
            else:
                self.LOGGER.info("Not enough days from last analysis, stopping.")

    def __get_logger(self):
        # create logger
        logger = logging.getLogger("MostCommonWords")
        logger.setLevel(logging.DEBUG)
        # create console handler and set level to debug
        log_path = "../log/most_commond_words.log"
        if not os.path.isdir("../log/"):
            os.mkdir("../log/")
        fh = logging.FileHandler(log_path)
        fh.setLevel(logging.DEBUG)
        # create formatter
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        return logger


if __name__ == "__main__":
    most_frequent_words = MostFrequentWords()
    most_frequent_words.main()
