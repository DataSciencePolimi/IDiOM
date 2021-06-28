from pymongo import MongoClient
from datetime import datetime
from sklearn.decomposition import PCA
from pymongo.errors import CursorNotFound, ServerSelectionTimeoutError

import numpy as np
import umap
import logging
import yaml
import os


class DimReductionProcess:
    def __init__(self):
        with open("configuration/configuration.yaml") as f:
            self.CONFIG = yaml.load(f, Loader=yaml.FullLoader)
        # mongourl = self.CONFIG["mongourl"]
        mongourl = "mongodb://admin:adminpassword@localhost:27017"
        self.MONGO_CLIENT = MongoClient(mongourl)
        self.START_YEAR = 2020
        self.START_MONTH = 1
        self.END_YEAR = 2020
        self.END_MONTH = 2
        self.START = datetime(self.START_YEAR, self.START_MONTH, 1, 0, 0)
        self.END = datetime(self.END_YEAR, self.END_MONTH, 1, 0, 0)
        self.LOGGER = self.__get_logger()

    def db_news_extraction(self, lang, query, chunk_size, limit=0):
        if lang != "it":
            name_coll = "article_" + lang
        else:
            name_coll = "article"
        collection = self.MONGO_CLIENT["news"][name_coll]
        # Limit = 0 => No limit
        not_processed_docs = collection.find(
            query, no_cursor_timeout=True, batch_size=chunk_size
        ).limit(limit)
        return collection, not_processed_docs

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

    def reduce_dim(self, docs, n_dims):
        pca = PCA(n_components=n_dims, random_state=7)
        res_pca = pca.fit_transform(docs)
        # print("Shape after PCA: ", res_pca.shape)
        reducer = umap.UMAP(n_neighbors=n_dims, min_dist=0.5)
        res_umap = reducer.fit_transform(res_pca)
        # print("Shape after UMAP: ", res_umap.shape)
        return res_umap

    def build_query(self):
        q = {
            "$and": [
                {"discoverDate": {"$gte": self.START, "$lt": self.END}},
                {"bertEncoding": {"$exists": True}},
                {"parsedText": {"$ne": ""}},
                {"$or": [{"written": {"$exists": False}}, {"written": False}]},
            ]
        }
        return q

    def update_docs(self, collection, doc):
        query = {"_id": doc["_id"]}
        emb = doc["embedding"].astype("float")
        emb = emb.tolist()
        newvalues = {"$set": {"reducedEmbedding": emb, "written": True}}
        collection.update_one(query, newvalues)

    def mark_copied(self, collection, doc_id):
        query = {"_id": doc_id}
        new_values = {"$set": {"written": True}}
        collection.update_one(query, new_values)

    def update_dates(self):
        self.START_MONTH += 1
        self.END_MONTH += 1
        if self.START_MONTH == 13:
            self.START_MONTH = 1
            self.START_YEAR += 1
        if self.END_MONTH == 13:
            self.END_MONTH = 1
            self.END_YEAR += 1
        self.START = datetime(self.START_YEAR, self.START_MONTH, 1, 0, 0)
        self.END = datetime(self.END_YEAR, self.END_MONTH, 1, 0, 0)

    def reset_dates(self):
        self.START_YEAR = 2020
        self.START_MONTH = 1
        self.END_YEAR = 2020
        self.END_MONTH = 2
        self.START = datetime(self.START_YEAR, self.START_MONTH, 1, 0, 0)
        self.END = datetime(self.END_YEAR, self.END_MONTH, 1, 0, 0)

    def main(self):
        print("=" * 120)
        print("STARTED DIMENSIONALITY REDUCTION")

        lang = "en"
        n_dims = 50
        bert_embedding_size = 768

        chunk_size = 5000

        alternative = False

        print("CURRENT COLLECTION: ARTICLE {}".format(lang.upper()))
        while self.END.year <= 2020 or (self.END.year <= 2021 and self.END.month <= 1):
            try:
                query = self.build_query()
                coll, not_processed_docs = self.db_news_extraction(lang, query, chunk_size)
                count_per_month = coll.count_documents(query)
                if count_per_month < n_dims:
                    n_dims = count_per_month
                else:
                    n_dims = 50
                print("Found {} articles".format(count_per_month))
                print("Starting parsing docs from {}".format(self.START.strftime("%b_%Y")))

                chunks = self.yield_rows(not_processed_docs, chunk_size)

                folder = "temp_dim_reduction"
                file_name = "{}.npy".format(self.START.strftime("%b_%Y"))
                complete_path = "{}/{}".format(folder, file_name)

                docs = []
                for chunk in chunks:
                    # print("Processing chunk {}".format(chunk_idx))
                    for doc in chunk:
                        if (
                            doc["bertEncoding"] is not None
                            and not len(doc["bertEncoding"]) == 0
                        ):
                            elem = {}
                            elem["_id"] = doc["_id"]
                            elem["embedding"] = doc["bertEncoding"]
                            docs = np.append(docs, elem)
                if alternative and count_per_month == 0:
                    # open file for that month
                    try:
                        os.mkdir(folder)
                        print("Created {}".format(folder))
                    except Exception:
                        self.LOGGER.error("{} already exists".format(folder))

                    # Reset file at the beginning of processing
                    if self.START.year == 2020 and self.START.month == 1:
                        np.save(complete_path, docs)
                    else:
                        data = np.load(complete_path, allow_pickle=True)
                        # append to it new chunk
                        updated_data = np.append(data, docs)
                        np.save(complete_path, updated_data)
                    # update "written" field on db
                    for d in docs:
                        self.mark_copied(coll, d["_id"])

                print("Reducing dims for {}".format(self.START.strftime("%b_%Y")))

                if alternative:
                    docs = np.load(complete_path, allow_pickle=True)

                if len(docs) > 0:
                    # print("Found some articles")
                    to_reduce = np.reshape(
                        [d["embedding"] for d in docs], (-1, bert_embedding_size)
                    )
                    # print("Reducing dimensions of {}".format(to_reduce.shape))
                    results = self.reduce_dim(to_reduce, n_dims)
                    # print(results.shape)
                    for r in range(results.shape[0]):
                        docs[r]["embedding"] = results[r]
                        self.update_docs(coll, docs[r])

                # Reset & update for next iteration
                alternative = False
                self.update_dates()
                not_processed_docs.close()
            except CursorNotFound:
                self.LOGGER.error("Lost cursor for {}".format(self.START.strftime("%b_%Y")))
                self.LOGGER.error("Try alternative way...")
                alternative = True

        self.reset_dates()

    def __get_logger(self):
        # create logger
        logger = logging.getLogger("DimensionalityReduction")
        logger.setLevel(logging.DEBUG)
        # create console handler and set level to debug
        log_path = "log/bert_dim_reduction.log"
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
    dim_red_process = DimReductionProcess()
    dim_red_process.main()
