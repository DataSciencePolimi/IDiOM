import spacy
import os
import yaml

from pymongo import MongoClient
from itertools import chain
from datetime import datetime

from core_modules.topic_extraction.lda_module import LdaModule

"""
    Database functions
"""


def db_news_extraction(client, lang, query, chunk_size, limit=0):
    if lang != "it":
        name_coll = "article_" + lang
    else:
        name_coll = "article"
    collection = client["news"][name_coll]

    not_processed_docs = collection.find(
        query, no_cursor_timeout=True, batch_size=chunk_size
    ).limit(limit)
    return collection, not_processed_docs


def build_query(start, end):
    q = {"discoverDate": {"$gte": start, "$lt": end}}
    return q


def yield_rows(cursor, chunk_size):
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


def update_dates(start, end):
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


"""
    Documents parsing
"""


def parse_text(nlp, raw_data):
    doc = nlp(raw_data)
    # Retrieve sentences
    sentences = sentence_tokenize(doc)
    # Lemmatize + remove stop words
    lemmas = lemmatize_tokens(nlp, sentences)
    # Flatten results into a single list
    parsed_text = flatten_list(lemmas)

    return parsed_text


def fix_stop_words(lang, nlp):
    for word in nlp.Defaults.stop_words:
        nlp.vocab[word].is_stop = True
    if lang == "it":
        nlp.vocab["dio"].is_stop = True
    elif lang == "de":
        nlp.vocab["Prozent"].is_stop = True
    return


def add_custom_stop_words(nlp, custom_stop_words):
    for cw in custom_stop_words:
        nlp.vocab[cw].is_stop = True
    return


def sentence_tokenize(data):
    return [sent for sent in data.sents]


def lemmatize_tokens(nlp, data):
    lemmas = []
    for sent in data:
        sent_tokens = []
        for token in sent:
            candidate = token.lemma_.replace("’", "")
            if (
                not nlp.vocab[candidate].is_stop
                and not token.is_punct
                and len(candidate) > 1
                and not candidate.isspace()
            ):
                sent_tokens.append(candidate)
        lemmas.append(sent_tokens)
        sent_tokens = []
    return lemmas


def flatten_list(data):
    return list(chain.from_iterable(data))


def main():
    with open("configuration/configuration.yaml") as f:
        CONFIG = yaml.load(f, Loader=yaml.FullLoader)

    # mongourl = "mongodb://admin:adminpassword@localhost:27017"
    mongourl = CONFIG["mongourl"]
    MONGO_CLIENT = MongoClient(mongourl)

    # lang = "it"
    chunk_size = 5000

    a = CONFIG["topic_extraction"]["model_params"]["alpha"]  # 0.01
    b = CONFIG["topic_extraction"]["model_params"]["beta"]  # 0.91
    passes = CONFIG["topic_extraction"]["model_params"]["passes"]  # 20
    num_topics = CONFIG["topic_extraction"]["model_params"]["num_topics"]  # 3

    for lang in CONFIG["collections_lang"]:
        print("Processing {}".format(lang))
        START_YEAR = 2020
        START_MONTH = 1
        END_YEAR = 2020
        END_MONTH = 2
        START = datetime(START_YEAR, START_MONTH, 1, 0, 0)
        END = datetime(END_YEAR, END_MONTH, 1, 0, 0)

        if lang == "en":
            nlp = spacy.load("en_core_web_md")
        else:
            nlp = spacy.load("{}_core_news_md".format(lang))

        fix_stop_words(lang, nlp)

        while END.year <= 2020 or (END.year <= 2021 and END.month <= 1):
            # Get documents from DB and parse them
            documents = []
            query = build_query(START, END)
            _, not_processed_docs = db_news_extraction(
                MONGO_CLIENT, lang, query, chunk_size, limit=5000
            )
            chunks = yield_rows(not_processed_docs, chunk_size)
            for chunk in chunks:
                for doc in chunk:
                    parsed_doc = parse_text(nlp, doc["text"])
                    documents.append(parsed_doc)

            if len(documents) > 0:
                # Create folder structure
                subfolder = "lda_{}".format(lang)

                module = LdaModule(
                    lang=lang,
                    num_docs=len(documents),
                    doc_collection=documents,
                    num_topics=num_topics,
                    trained=False,
                )
                module.build_dictionary()
                module.build_corpus()
                module.build_lda_model(num_topics=num_topics, passes=passes, alpha=a, eta=b)

                file_name = "lda_{}_{}".format(lang, START.strftime("%m_%Y"))
                module.save_LDA_model(subfolder, file_name)

            START, END = update_dates(START, END)
            not_processed_docs.close()


main()
