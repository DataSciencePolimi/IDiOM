import yaml
import logging
import requests
from pprint import pprint
import json
import os
import sys
import datetime
import random
import dateparser
from pymongo import MongoClient
import pytz
import bson


class NewsScraper:
    def __init__(self, lang):
        with open("../configuration/configuration.yaml", "r") as f:
            self.CONFIG = yaml.load(f, Loader=yaml.FullLoader)
        self.lang = lang
        self.lang_code = lang.lower()
        self.NEWS_API_LIST = self.CONFIG["scraper"]["newsriver_api"]
        self.LOGGER = self.__get_logger()
        self.sort_by = self.CONFIG["scraper"]["sort_by"]
        self.sort_order = self.CONFIG["scraper"]["sort_order"]
        self.CLIENT = MongoClient(self.CONFIG["mongourl"])
        self.used_tokens = {api: False for api in range(len(self.NEWS_API_LIST))}
        index_api = random.randint(0, len(self.NEWS_API_LIST) - 1)
        self.NEWS_API = self.NEWS_API_LIST[index_api]
        self.used_tokens[index_api] = True

    def __get_logger(self):
        # create logger
        logger = logging.getLogger("NewsScraper")
        logger.setLevel(logging.DEBUG)
        # create console handler and set level to debug
        if self.lang == "IT":
            log_path = "../log/news_scraper.log"
        else:
            log_path = "../log/news_scraper_" + self.lang_code + ".log"
        if not os.path.isdir("../log/"):
            os.mkdir("../log/")
        fh = logging.FileHandler(log_path)
        fh.setLevel(logging.DEBUG)
        # create formatter
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        return logger

    def get_new_api(self):
        for idx, used in self.used_tokens.items():
            if not used:
                self.used_tokens[idx] = True
                return idx
        return None

    def get_news_by_query(self, query, sort_by=None, sort_order=None, limit=None):
        # query is composed used Lucene syntax:
        # see https://lucene.apache.org/core/2_9_4/queryparsersyntax.html
        # and https://console.newsriver.io/code-book for details
        url = "https://api.newsriver.io/v2/search?query={}".format(query)
        if sort_by is not None:
            url += "&sortBy={}".format(self.sort_by[sort_by])
        if sort_order is not None:
            url += "&sortOrder={}".format(self.sort_order[sort_order])
        if limit is not None:
            url += "&limit={}".format(limit)
        try:
            response = requests.get(url, headers={"Authorization": self.NEWS_API})
            response_json = response.json()
            return response_json
        except Exception:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.LOGGER.error(
                "Response code: {}, Response text: {}, {}, {}, {}".format(
                    response.status_code,
                    response.text,
                    exc_type,
                    fname,
                    exc_tb.tb_lineno,
                )
            )
            return None

    def save_news_to_db(self):
        # print(self.NEWS_API)
        # get correct db collection
        print(self.lang)
        print(self.lang_code)
        if self.lang == "IT":
            collection = self.CLIENT["news"]["article"]
            current_date = self.CONFIG["scraper"]["start_date"]
        else:
            collection = self.CLIENT["news"]["article_" + self.lang_code]
            current_date = self.CONFIG["scraper"]["start_date_" + self.lang_code]
        stop = False
        self.stop_date = datetime.datetime.now()
        self.LOGGER.info("Starting scheduled news scraping...")
        self.LOGGER.info("Start Date: {}".format(current_date))
        while not stop:
            if self.lang == "TR":
                query = "language:{} AND text:coronavirus AND discoverDate:[{} TO *] ".format(
                    self.lang,
                    datetime.datetime.strftime(current_date, "%Y-%m-%dT%H:%M:%S.%f"),
                )
            else:
                query = "language:{} AND title:coronavirus AND discoverDate:[{} TO *] ".format(
                    self.lang,
                    datetime.datetime.strftime(current_date, "%Y-%m-%dT%H:%M:%S.%f"),
                )
            print(query)
            json_news = self.get_news_by_query(query, "discover_date", "asc", 100)
            if json_news is None:
                idx_api = self.get_new_api()
                if idx_api is None:
                    stop = True
                    self.LOGGER.error("Used all API keys, but none of them work")
                    break
                self.LOGGER.error("Retrying download news, changing API Token...")
                self.NEWS_API = self.CONFIG["scraper"]["newsriver_api"][idx_api]
            else:
                for news in json_news:
                    # print(type(news['discoverDate']))
                    # print(news['discoverDate'])
                    discover_date = dateparser.parse(news["discoverDate"]).replace(
                        tzinfo=None
                    )
                    news["discoverDate"] = discover_date
                    try:
                        if len(news["text"]) > 0:
                            collection.insert_one(news)
                    except Exception:
                        exc_type, exc_obj, exc_tb = sys.exc_info()
                        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                        self.LOGGER.error(
                            "Error on DB Insert: {}, {}, {}".format(
                                exc_type, fname, exc_tb.tb_lineno
                            )
                        )
                last_article = json_news[-1]
                stop = self.stop_condition(last_article)
                current_date = last_article["discoverDate"] + datetime.timedelta(
                    milliseconds=10
                )
                if self.lang == "IT":
                    self.CONFIG["scraper"]["start_date"] = last_article["discoverDate"]
                else:
                    self.CONFIG["scraper"][
                        "start_date_" + self.lang_code
                    ] = last_article["discoverDate"]
                with open("../configuration/configuration.yaml", "w") as f:
                    yaml.dump(self.CONFIG, f)
                self.LOGGER.info("Current Date: {}".format(current_date))
            # with open('data/articles.json', 'w') as w:
            #         json.dump(json_news,w)
        self.LOGGER.info("Scheduled News Scraping Done until {}".format(current_date))

    def stop_condition(self, article):
        # current_date = dateparser.parse(article['discoverDate']).
        # replace(tzinfo=None)
        if article["discoverDate"] >= self.stop_date:
            # if dateparser.parse(article['discoverDate']).
            # replace(tzinfo=None) >= self.stop_date:
            return True
        return False


if __name__ == "__main__":
    news_scraper = NewsScraper()
    # query = 'language:IT AND title:coronavirus'
    # json_news = news_scraper.get_news_by_query(query, 'discover_date', 'desc')
    # pprint(json_news)
    news_scraper.save_news_to_db()
