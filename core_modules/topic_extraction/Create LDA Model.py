#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# %load_ext nb_black


# In[ ]:


import sys
import yaml
import logging
import requests
from pprint import pprint
import json
import time
import os
from pymongo import MongoClient
import dateparser
import psutil
from pathlib import Path

sys.path.append(str(Path(os.getcwd())) + "/")
# print(sys.path)
from core_modules.topic_extraction.nlp_utils import NLPUtils
from core_modules.topic_extraction.lda_module import LdaModule


# In[ ]:


mongourl = "mongodb://localhost:27017/"
MONGO_CLIENT = MongoClient(mongourl)
LANG_CODE = "nl"


# In[ ]:


collection = MONGO_CLIENT["news"]["article" + "_" + LANG_CODE]
# collection = MONGO_CLIENT["news"]["article"]
not_processed_docs = collection.find()
# for doc in not_processed_docs:
#    print(doc)
#    time.sleep(10)


# In[ ]:


training_set = []
# i = 0
for document in not_processed_docs:
    # if i % 10000 == 0:
    #     print(i)
    #     print(psutil.virtual_memory())
    training_set.append(document["text"])
    # i += 1


# In[ ]:


nlp_utils = NLPUtils(lang=LANG_CODE)


# In[ ]:


# Some preparation before running LDA
doc_collection = []
print("Parsing articles...")
i = 0
for doc in training_set[:1000]:
    if i % 100 == 0:
        print(i)
    tokens = nlp_utils.parse_text(doc)
    doc_collection.append(tokens)
    i += 1


# In[ ]:


print("Completed parsing {} articles".format(len(doc_collection)))

num_docs = len(doc_collection)
num_topics = 20
lda = LdaModule(
    lang=LANG_CODE,
    num_docs=num_docs,
    doc_collection=doc_collection,
    num_topics=num_topics,
    trained=False,
)
lda.runLDA()

docs_topics_dict = lda.get_docs_topics_dict()


# In[ ]:


# == Saving model checkpoint ==
lda.save_LDA_model()


# In[ ]:
