from datetime import datetime
from pymongo import MongoClient
from core_modules.topic_extraction.nlp_utils import NLPUtils
from core_modules.topic_extraction.lda_module import LdaModule
from pymongo.errors import CursorNotFound, ServerSelectionTimeoutError

import os
import logging
import yaml
import sys
import gc


class FixTopicProcess:
    def __init__(self):
        # init
        with open("configuration/configuration.yaml") as f:
            self.CONFIG = yaml.load(f, Loader=yaml.FullLoader)
        self.LOGGER = self.__get_logger()
        # mongourl = "mongodb://admin:adminpassword@localhost:27017"
        mongourl = self.CONFIG["mongourl"]
        self.MONGO_CLIENT = MongoClient(mongourl)
        self.lda_module = None
        self.nlp_utils = None

    def build_query(self, start, end):
        q = {
            "$and": [
                {"discoverDate": {"$gte": start, "$lt": end}},
                {
                    "$or": [
                        {"processedEncoding": {"$exists": False}},
                        {"processedEncoding": False},
                    ]
                },
            ]
        }
        return q

    def yield_rows(self, cursor, chunk_size):
        """
        Generator to yield chunks from cursor
        :param cursor:
        :param chunk_size:
        :return:
        """
        chunk = []
        for i, row in enumerate(cursor):
            if i % chunk_size == 0 and i > 0:
                yield chunk
                del chunk[:]
            chunk.append(row)
        yield chunk

    def update_dates(self, start, end):
        start_month = start.month + 1
        start_year = start.year
        end_month = end.month + 1
        end_year = end.year
        if start_month == 13:
            start_month = 1
            start_year = start.year + 1
        if end_month == 13:
            end_month = 1
            end_year = end.year + 1
        new_start = datetime(start_year, start_month, 1, 0, 0)
        new_end = datetime(end_year, end_month, 1, 0, 0)
        return new_start, new_end

    def db_news_extraction(self, lang, query, chunk_size, limit=0):
        if lang != "it":
            name_coll = "article_" + lang
        else:
            name_coll = "article"
        collection = self.MONGO_CLIENT["news"][name_coll]
        not_processed_docs = collection.find(
            query, no_cursor_timeout=True, batch_size=chunk_size
        ).limit(limit)
        return collection, not_processed_docs

    def process_doc(self, doc):
        # doc["text"] = doc["text"][:10000]  # temp fix ^ 2
        # topic extraction phase
        try:
            doc = self.topic_extraction(doc)
        except Exception:
            exc_type, _, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.LOGGER.error(
                "{}, {}, {}, {}".format(doc["_id"], exc_type, fname, exc_tb.tb_lineno)
            )
            return None, "error"
        gc.collect()

        return doc, None

    def topic_extraction(self, doc):
        parsed_text = self.nlp_utils.parse_text(doc)

        try:
            topics = self.lda_module.model.show_topics(
                formatted=False,
                num_topics=self.CONFIG["topic_extraction"]["model_params"]["num_topics"],
                num_words=self.CONFIG["topic_extraction"]["model_params"]["num_words"],
            )
        except Exception:
            exc_type, _, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.LOGGER.error(
                "{}, {}, {}, {}".format(doc["_id"], exc_type, fname, exc_tb.tb_lineno)
            )
            raise Exception("Error on lda module show topics")

        # Formatting
        document_topic_info = []
        for el in self.lda_module.model[self.lda_module.dictionary.doc2bow(parsed_text)]:
            new_entry = {}
            new_entry["topic_number"] = str(el[0])
            new_entry["topic_prob"] = float(round(el[1], 2))
            new_entry["topic_tokens"] = self.lda_module.format_topic_list(topics[el[0]][1])
            document_topic_info.append(new_entry)

        doc["topic_extraction"] = document_topic_info
        doc["parsed_text"] = " ".join(word for word in parsed_text)
        return doc

    def db_news_update(self, collection, doc, empty=False):
        query = {"_id": doc["_id"]}
        if empty:
            newvalues = {
                "$set": {"parsedText": "", "topicExtraction": [], "processedEncoding": True}
            }
        else:
            newvalues = {
                "$set": {
                    "parsedText": doc["parsed_text"],
                    "topicExtraction": doc["topic_extraction"],
                    "processedEncoding": True,
                }
            }
        collection.update_one(query, newvalues)

    def main(self):
        self.LOGGER.info("=" * 120)
        self.LOGGER.info("STARTED POST PROCESSING")

        chunk_size = 5000

        for lang in self.CONFIG["collections_lang"]:
            self.LOGGER.info("CURRENT COLLECTION: ARTICLE {}".format(lang.upper()))
            self.LOGGER.info("Initializing core modules and extract news from db...")

            start_year = 2020
            start_month = 1
            end_year = 2020
            end_month = 2
            start = datetime(start_year, start_month, 1, 0, 0)
            end = datetime(end_year, end_month, 1, 0, 0)

            self.nlp_utils = None
            self.nlp_utils = NLPUtils(lang=lang)

            while end.year <= 2020 or (end.year <= 2021 and end.month <= 1):
                query = self.build_query(start, end)
                collection, not_processed_docs = self.db_news_extraction(
                    lang, query, chunk_size
                )

                # Create empty than load
                self.lda_module = None
                self.lda_module = LdaModule()
                self.lda_module.load_lda_model(lang, start)
                # print("Topics for {}".format(start.strftime("%b_%Y")))
                # t = self.lda_module.model.show_topics(
                #     formatted=True,
                #     num_topics=self.CONFIG["topic_extraction"]["model_params"]["num_topics"],
                #     num_words=self.CONFIG["topic_extraction"]["model_params"]["num_words"],
                # )
                # print(t)
                # print("=" * 20)

                i = 0
                self.LOGGER.info(
                    "Starting processing docs from {}".format(start.strftime("%b_%Y"))
                )
                try:
                    chunks = self.yield_rows(not_processed_docs, chunk_size)
                    for chunk in chunks:
                        for doc in chunk:
                            if i % 1000 == 0 and i > 0:
                                print(i)
                                gc.collect()
                            if i % 10000 == 0 and i > 0:
                                self.LOGGER.info("10k Docs processed...")
                            if len(doc["text"]) > 0 and not doc["text"].isspace():
                                updated_doc, error = self.process_doc(doc)

                                if error is None:
                                    self.db_news_update(collection, updated_doc, empty=False)
                            else:
                                self.db_news_update(collection, doc, empty=True)
                            i = i + 1
                    start, end = self.update_dates(start, end)
                except (CursorNotFound, ServerSelectionTimeoutError) as e:
                    self.LOGGER.error("{} occurred. Retry...".format(e))
                not_processed_docs.close()

    def __get_logger(self):
        # create logger
        logger = logging.getLogger("FixTopicProcess")
        logger.setLevel(logging.DEBUG)
        # create console handler and set level to debug
        log_path = "log/fix_topic_process.log"
        if not os.path.isdir("log/"):
            os.mkdir("log/")
        fh = logging.FileHandler(log_path)
        fh.setLevel(logging.DEBUG)
        # create formatter
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        return logger


if __name__ == "__main__":
    fix_topic_process = FixTopicProcess()
    fix_topic_process.main()
