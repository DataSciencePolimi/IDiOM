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
from dateutil.relativedelta import relativedelta
import re
import dateutil.parser as parser
import pandas as pd


class DBHandler:
    def __init__(self):
        with open("configuration/configuration.yaml") as f:
            self.CONFIG = yaml.load(f, Loader=yaml.FullLoader)
        self.LOGGER = self.__get_logger()
        mongourl = self.CONFIG["mongourl"]
        self.MONGO_CLIENT = MongoClient(mongourl)

    def get_common_words(self, start_date, lang):
        if type(start_date) is str:
            start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        end_date = start_date + relativedelta(months=1) - datetime.timedelta(days=1)
        collection = self.MONGO_CLIENT["statistics"]["month_" + lang]
        ts = int(datetime.datetime.timestamp(end_date))
        query = {"ts": ts}
        # self.LOGGER.info(query)
        documents = collection.find(query)
        if documents is not None:
            res = {"data": []}
            for doc in documents:
                date_range = doc["dateRange"]
                date_range = date_range.split("00:00:00-")
                for i in range(len(date_range)):
                    date_range[i] = date_range[i].replace("00:00:00", "").strip()
                res["data"].append(
                    {
                        "date_range": "{}__{}".format(date_range[0], date_range[1]),
                        "most_frequent_words": doc["most_frequent_words"],
                    }
                )
            return res
        return None

    def get_articles_per_day(self, start_date, lang):
        try:
            if type(start_date) is str:
                start_date = datetime.datetime.strptime(start_date, "%Y-%m")
            # self.LOGGER.info(start_date)
            end_date = start_date + relativedelta(months=1) - datetime.timedelta(days=1)
            # self.LOGGER.info(end_date)
            if lang == "it":
                collection = self.MONGO_CLIENT["news"]["article"]
            else:
                collection = self.MONGO_CLIENT["news"]["article_" + lang]

            documents = collection.aggregate(
                [
                    {"$match": {"discoverDate": {"$gte": start_date, "$lte": end_date}}},
                    {
                        "$group": {
                            "_id": {
                                "$dateToString": {
                                    "format": "%Y-%m-%d",
                                    "date": "$discoverDate",
                                }
                            },
                            "count": {"$sum": 1},
                        }
                    },
                    {
                        "$sort": {
                            "_id": pymongo.ASCENDING,
                        }
                    },
                ]
            )

            date_db = []
            count_db = []

            for doc in documents:
                date_db.append(datetime.datetime.strptime(doc["_id"], "%Y-%m-%d"))
                count_db.append(doc["count"])

            data = dict(date=date_db, count=count_db)
            return data
        except Exception as e:
            exc_type, _, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.LOGGER.error(
                "{}, {}, {}, {}, {}".format(
                    start_date, exc_type, fname, exc_tb.tb_lineno, str(e)
                )
            )
            return None

    def get_articles_topic_count(self, start_date, lang):
        try:
            if type(start_date) is str:
                start_date = datetime.datetime.strptime(start_date, "%Y-%m")
            # self.LOGGER.info(start_date)
            end_date = start_date + relativedelta(months=1) - datetime.timedelta(days=1)
            # self.LOGGER.info(end_date)
            if lang == "it":
                collection = self.MONGO_CLIENT["news"]["article"]
            else:
                collection = self.MONGO_CLIENT["news"]["article_" + lang]

            documents = collection.aggregate(
                [
                    {"$match": {"discoverDate": {"$gte": start_date, "$lte": end_date}}},
                ]
            )

            count_topic_0 = 0
            count_topic_1 = 0
            count_topic_2 = 0

            for doc in documents:
                if len(doc["topicExtraction"]) == 0:
                    continue
                if len(doc["topicExtraction"]) == 1:
                    if int(doc["topicExtraction"][0]["topic_number"]) == 0:
                        count_topic_0 = count_topic_0 + 1
                    elif int(doc["topicExtraction"][0]["topic_number"]) == 1:
                        count_topic_1 = count_topic_1 + 1
                    elif int(doc["topicExtraction"][0]["topic_number"]) == 2:
                        count_topic_2 = count_topic_2 + 1
                else:
                    max = 0
                    max_topic = None
                    for topic in doc["topicExtraction"]:
                        if topic["topic_prob"] > max:
                            max = topic["topic_prob"]
                            max_topic = int(topic["topic_number"])

                    if max_topic is not None:
                        if max_topic == 0:
                            count_topic_0 = count_topic_0 + 1
                        elif max_topic == 1:
                            count_topic_1 = count_topic_1 + 1
                        elif max_topic == 2:
                            count_topic_2 = count_topic_2 + 1

            data = dict(
                topic=["Topic 1", "Topic 2", "Topic 3"],
                count=[count_topic_0, count_topic_1, count_topic_2],
            )
            return data
        except Exception as e:
            exc_type, _, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.LOGGER.error(
                "{}, {}, {}, {}, {}".format(
                    start_date, exc_type, fname, exc_tb.tb_lineno, str(e)
                )
            )
            return None

    def get_reduced_articles(self, start_date, lang):
        try:
            if type(start_date) is str:
                start_date = datetime.datetime.strptime(start_date, "%Y-%m")
            # self.LOGGER.info(start_date)
            end_date = start_date + relativedelta(months=1) - datetime.timedelta(days=1)
            # self.LOGGER.info(end_date)
            if lang == "it":
                collection = self.MONGO_CLIENT["news"]["article"]
            else:
                collection = self.MONGO_CLIENT["news"]["article_" + lang]

            query = {"discoverDate": {"$gte": start_date, "$lte": end_date}}

            documents = collection.find(query, no_cursor_timeout=True)

            x_db = []
            y_db = []
            date_db = []
            title_db = []
            topic_db = []

            for doc in documents:
                if len(doc["reducedEmbedding"]) > 0:
                    x_db.append(doc["reducedEmbedding"][0])
                    y_db.append(doc["reducedEmbedding"][1])
                    date_db.append(doc["discoverDate"])
                    title_db.append(doc["title"])
                    topic_db.append(doc["topicExtraction"])

            data = dict(x=x_db, y=y_db, date=date_db, title=title_db, topic=topic_db)
            return data
        except Exception as e:
            exc_type, _, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.LOGGER.error(
                "{}, {}, {}, {}, {}".format(
                    start_date, exc_type, fname, exc_tb.tb_lineno, str(e)
                )
            )
            return None

    def group_by_ner(self, entity_name, frequency, n):

        # Creazione Dataframe e Rimozione Duplicati
        data = {"entity_name": entity_name, "frequency": frequency}

        data_df = pd.DataFrame(data, columns=["entity_name", "frequency"])
        data_df_no_dups = (
            data_df.groupby(["entity_name"])
            .sum()
            .reset_index()
            .sort_values(["frequency"], ascending=False)
        )

        data_df_en_array = []
        data_df_f_array = []

        for en in data_df_no_dups["entity_name"][:n]:
            data_df_en_array.append(en)

        for f in data_df_no_dups["frequency"][:n]:
            data_df_f_array.append(f)

        return (data_df_en_array, data_df_f_array)

    def get_most_frequent_ner(self, start_date, lang, n):

        try:
            if type(start_date) is str:
                start_date = datetime.datetime.strptime(start_date, "%Y-%m")
            # self.LOGGER.info(start_date)
            end_date = start_date + relativedelta(months=1) - datetime.timedelta(days=1)
            # self.LOGGER.info(end_date)
            if lang == "it":
                collection = self.MONGO_CLIENT["news"]["article"]
            else:
                collection = self.MONGO_CLIENT["news"]["article_" + lang]

            query = {"discoverDate": {"$gte": start_date, "$lte": end_date}}

            documents = collection.find(query, no_cursor_timeout=True)

            entity_name = []
            frequency = []

            for doc in documents:
                for ner_element in doc["namedEntityRecognition"]:
                    if ner_element["entity_name"] != "":
                        entity_name.append(ner_element["entity_name"])
                        frequency.append(ner_element["freq"])

            entity_name, frequency = self.group_by_ner(entity_name, frequency, n)

            data = dict(entity_name=entity_name, frequency=frequency)
            return data
        except Exception as e:
            exc_type, _, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.LOGGER.error(
                "{}, {}, {}, {}, {}".format(
                    start_date, exc_type, fname, exc_tb.tb_lineno, str(e)
                )
            )
            return None

        return None

    def time_series_count_most_frequent_ner_articles(self, start_date, lang):

        try:
            if type(start_date) is str:
                start_date = datetime.datetime.strptime(start_date, "%Y-%m")
            # self.LOGGER.info(start_date)
            end_date = start_date + relativedelta(months=1) - datetime.timedelta(days=1)
            # self.LOGGER.info(end_date)
            if lang == "it":
                collection = self.MONGO_CLIENT["news"]["article"]
            else:
                collection = self.MONGO_CLIENT["news"]["article_" + lang]

            query = {"discoverDate": {"$gte": start_date, "$lte": end_date}}

            documents = collection.find(query, no_cursor_timeout=True)

            entity_name = []
            frequency = []

            for doc in documents:
                for ner_element in doc["namedEntityRecognition"]:
                    if ner_element["entity_name"] != "":
                        entity_name.append(ner_element["entity_name"])
                        frequency.append(ner_element["freq"])

            entity_name, frequency = self.group_by_ner(entity_name, frequency)

            documents_2 = collection.aggregate(
                [
                    {
                        "$match": {
                            "discoverDate": {"$gte": start_date, "$lte": end_date},
                            "namedEntityRecognition.entity_name": {"$in": entity_name},
                        }
                    },
                    {
                        "$group": {
                            "_id": {
                                "$dateToString": {
                                    "format": "%Y-%m-%d",
                                    "date": "$discoverDate",
                                }
                            },
                            "count": {"$sum": 1},
                        }
                    },
                    {
                        "$sort": {
                            "_id": pymongo.ASCENDING,
                        }
                    },
                ]
            )

            date = []
            count = []

            for el in documents_2:
                date.append(el["_id"])
                count.append(el["count"])

            final_date = []
            final_count = []
            s_day = start_date
            e_day = end_date
            while s_day < e_day:

                if s_day.strftime("%Y-%m-%d") in date:
                    pos = date.index(s_day.strftime("%Y-%m-%d"))
                    final_date.append(datetime.datetime.strptime(date[pos], "%Y-%m-%d"))
                    final_count.append(count[pos])

                else:
                    final_date.append(
                        datetime.datetime.strptime(s_day.strftime("%Y-%m-%d"), "%Y-%m-%d")
                    )
                    final_count.append(0)

                s_day += datetime.timedelta(days=1)

            data = dict(date=final_date, count=final_count)
            return data
        except Exception as e:
            exc_type, _, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.LOGGER.error(
                "{}, {}, {}, {}, {}".format(
                    start_date, exc_type, fname, exc_tb.tb_lineno, str(e)
                )
            )
            return None

        return None

    def __get_logger(self):
        # create logger
        logger = logging.getLogger("DBHandler")
        logger.setLevel(logging.DEBUG)
        # create console handler and set level to debug
        log_path = "log/db_handler.log"
        if not os.path.isdir("log/"):
            os.mkdir("log/")
        fh = logging.FileHandler(log_path)
        fh.setLevel(logging.DEBUG)
        # create formatter
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        return logger
