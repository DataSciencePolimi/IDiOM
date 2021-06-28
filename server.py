import json
from bokeh.embed import json_item
import yaml
import os
import logging
import sys
from utils.db_handler import DBHandler
from functools import wraps
from representations.graphs import Graphs


class MyServer:
    def __init__(self):
        with open("configuration/config_server.yaml") as f:
            self.CONFIG = yaml.load(f, Loader=yaml.FullLoader)
        self.allowed_ip = self.CONFIG["allowed_ip"]
        self.api_tokens = self.CONFIG["api_tokens"]
        self.LOGGER = self.__get_logger()

    def __get_logger(self):
        # create logger
        logger = logging.getLogger("ServerLog")
        logger.setLevel(logging.DEBUG)
        # create console handler and set level to debug
        log_path = "log/server_log.log"
        if not os.path.isdir("log/"):
            os.mkdir("log/")
        fh = logging.FileHandler(log_path)
        fh.setLevel(logging.DEBUG)
        # create formatter
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        return logger


from flask import Flask, request, abort, jsonify
from flask_httpauth import HTTPTokenAuth
from waitress import serve

app = Flask(__name__)
auth = HTTPTokenAuth(scheme="Bearer")
my_server = MyServer()
db_handler = DBHandler()
graphs = Graphs()

### Decorators and error code handlers related functions ###


@auth.verify_token
def verify_token(token):
    # my_server.LOGGER.info(token)
    # my_server.LOGGER.info(my_server.api_tokens)
    if token in my_server.api_tokens:
        return my_server.api_tokens[token]


def check_ip(f):
    @wraps(f)
    def wrapped(*args, **kwargs):
        ip_client = request.environ.get("HTTP_X_REAL_IP", request.remote_addr)
        if ip_client in my_server.allowed_ip:
            # my_server.LOGGER.info(
            #     "allowed - {} - {}".format(request.remote_addr, my_server.allowed_ip)
            # )
            return f(*args, **kwargs)
        else:
            return abort(401)

    return wrapped


@app.errorhandler(401)
def not_authorized(e):
    return jsonify(error=str(e)), 401


@app.errorhandler(400)
def bad_request(e):
    return jsonify(error=str(e)), 400


@app.errorhandler(404)
def not_found(e):
    return jsonify(error=str(e)), 404


@app.route("/plot_topic_count")
@auth.login_required
@check_ip
def plot_topic_count():
    if "date" not in request.args or "lang" not in request.args:
        abort(400)
    else:

        topic_count_per_month = db_handler.get_articles_topic_count(
            request.args["date"], request.args["lang"]
        )

        my_server.LOGGER.info(topic_count_per_month)

        if topic_count_per_month is not None:

            layout = graphs.create_articles_count_per_topic(topic_count_per_month)

            response = app.response_class(
                response=json.dumps(json_item(layout, "myplot")), mimetype="application/json"
            )

            return response
        else:
            my_server.LOGGER.info("Something went wrong")
            return "Something went wrong"


@app.route("/plot_articles")
@auth.login_required
@check_ip
def plot_articles():
    if "date" not in request.args or "lang" not in request.args:
        abort(400)
    else:

        reduced_articles = db_handler.get_reduced_articles(
            request.args["date"], request.args["lang"]
        )

        if reduced_articles is not None:

            layout = graphs.create_article_graph_with_sliders_and_filters(
                reduced_articles, request.args["date"]
            )

            response = app.response_class(
                response=json.dumps(json_item(layout, "myplot")), mimetype="application/json"
            )

            return response
        else:
            my_server.LOGGER.info("Something went wrong")
            return "Something went wrong"


@app.route("/plot_articles_time_series")
@auth.login_required
@check_ip
def plot_articles_time_series():
    if "date" not in request.args or "lang" not in request.args:
        abort(400)
    else:
        articles_per_day = db_handler.get_articles_per_day(
            request.args["date"], request.args["lang"]
        )

        if articles_per_day is not None:

            layout = graphs.create_article_time_series(articles_per_day)

            response = app.response_class(
                response=json.dumps(json_item(layout, "myplot")), mimetype="application/json"
            )

            return response
        else:
            my_server.LOGGER.info("Something went wrong")
            return "Something went wrong"


@app.route("/plot_most_frequent_ner_wordcloud")
@auth.login_required
@check_ip
def plot_most_frequent_ner_wordcloud():
    if "date" not in request.args or "lang" not in request.args:
        abort(400)
    else:
        most_frequent_ner = db_handler.get_most_frequent_ner(
            request.args["date"], request.args["lang"], 20
        )

        if most_frequent_ner is not None:

            layout = graphs.create_most_frequent_ner_wordcloud(most_frequent_ner)

            # layout = {}

            # response = app.response_class(
            #     response=json.dumps(json_item(layout, "myplot")), mimetype="application/json"
            # )

            # return response
        else:
            my_server.LOGGER.info("Something went wrong")
            return "Something went wrong"


@app.route("/plot_most_frequent_ner")
@auth.login_required
@check_ip
def plot_most_frequent_ner():
    if "date" not in request.args or "lang" not in request.args:
        abort(400)
    else:
        most_frequent_ner = db_handler.get_most_frequent_ner(
            request.args["date"], request.args["lang"], 10
        )

        if most_frequent_ner is not None:

            layout = graphs.create_most_frequent_ner(most_frequent_ner)

            response = app.response_class(
                response=json.dumps(json_item(layout, "myplot")), mimetype="application/json"
            )

            return response
        else:
            my_server.LOGGER.info("Something went wrong")
            return "Something went wrong"


@app.route("/plot_articles_time_series_mfner")
@auth.login_required
@check_ip
def plot_articles_time_series_mfner():
    if "date" not in request.args or "lang" not in request.args:
        abort(400)
    else:
        articles_mfner = db_handler.time_series_count_most_frequent_ner_articles(
            request.args["date"], request.args["lang"]
        )

        if articles_mfner is not None:

            layout = graphs.create_article_ts_mfner(articles_mfner)

            response = app.response_class(
                response=json.dumps(json_item(layout, "myplot")), mimetype="application/json"
            )

            return response
        else:
            my_server.LOGGER.info("Something went wrong")
            return "Something went wrong"


# @app.route("/plot_flower")
# @auth.login_required
# @check_ip
# def plot():
#     p = make_plot_flower("petal_width", "petal_length")

#     response = app.response_class(
#         response=json.dumps(json_item(p, "myplot")), mimetype="application/json"
#     )
#     return response


# @app.route("/")
# @auth.login_required
# @check_ip
# def index():
#     return "Hello, {}!".format(auth.current_user())


@app.route("/common_words")
@auth.login_required
@check_ip
def common_words():
    if "date" not in request.args or "lang" not in request.args:
        abort(400)
    else:
        common_words = db_handler.get_common_words(request.args["date"], request.args["lang"])
        if common_words is not None:
            response = app.response_class(
                response=json.dumps(common_words), mimetype="application/json"
            )
            return response
        else:
            return "Something went wrong"


if __name__ == "__main__":
    serve(app, host="0.0.0.0", url_scheme="https")
