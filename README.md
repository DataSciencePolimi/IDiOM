
# IDiOM

  

IDiOM Is a comprehensive set of Data Science tools integrated in a flawless pipeline for Information &amp; Disinformation Analysis in Online Media

Our work focuses on techniques of **data linking** and **analysis** between social media and news, with a try of integrating of Open Government Data (OGD) as credible information to assist users in evaluating the authenticity of unverified information, using methods of **natural language processing** and **machine learning**. The final purpose is to create a publicly available dashboard and to give to citizens a tool to become aware of misinformation and understand where to keep informed in an appropriate way.

## Method

The scope of the work is to achieve a complex but flexible pipeline, capable of handling different scenarios and types of data.

The **spectrum of sub problems** to solve in order to efficiently capture all these aspects is extremely variegated:

- Entity detection and recognition, entity linking and knowledge base management
- Topic extraction on different types of sentences  
- Semantic similarity between different sentences  
- Behaviour analysis
- Opinion and sentiment mining
- Triples Extraction from semantic structured data
- Knowledge Graph Building
- Temporal and spatial analysis of the phenomena  

A set of **techniques and methods** are extensively used to solve each of these problems:

- Neural Network Models  
- Deep Language Models, e.g., BERT  
- Graph Analysis and Graph Representation Learning Models  
- Classification methods, like Random Forest, Naïve Bayes Models, and SVM
- Clustering, Latent Dirichlet Allocation

The figure below shows an overview of the entire pipeline in an example scenario, which is extensible or modifiable by changing the types of data sources. After every module description, there is a short technical discussion about the frameworks that will be used to accomplish the task.

![Pipeline Architecture](https://github.com/DataSciencePolimi/IDiOM/blob/main/architecture.jpeg?raw=true)


### 1. Data Collection
It is the first part of the pipeline, where all the data are collected thanks to the publicly available APIs and the use of scraping techniques. Each type of data is downloaded and processed in the scraper engine; once the data takes on a standard structured form, it is saved to a database so that it is available for later steps.

### 2. Data Analysis
The post processing phase is the core part of the architecture, because it contains all the modules that require AI techniques and that allows to get the final data, ready to be visualized. Depending on the type of data this phase receives, the flow of modules to be applied is different, even if the modules can be reused on the different types of data.

**Named entity recognition (NER)** is the first step towards information extraction that seeks to locate and classify named entities in text into pre-defined categories such as the names of people, organizations, locations, expressions of times, quantities, monetary values, etc. This technique is applied to every type of data that comes to this stage of the pipeline, because having a classified text can be useful to better understand the semantic meaning of its sentences. In fact, with the triples subject, predicate, object extracted from text, a knowledge graph is built.

**Topic extraction** is Natural Language Processing technique used to discover hidden semantic structures of text in a collection of documents. Texts are represented as a distilled structure with respect to the topics the set of documents is about. In the proposed data analysis pipeline, this step is implemented via **LDA – Latent Dirichlet Allocation**. In this step the documents are processed as ‘bag-of-words’ and each token is tagged (noun, verb, adjective, etc.), reduced to its canonical form, paired with another if they co-occur often or discarded if too commonly used.

**Sentiment analysis** refers to the use of natural language processing, text analysis, computational linguistics, and bio-metrics to systematically identify, extract, quantify, and study affective states and subjective information. In this case the sentiment analysis module will process sentences that come from social media, articles and other data sources with the help of a language model called **BERT**, developed by Google research.

**Knowledge Graph** is typically built on top of the existing databases to link all data together at web-scale combining both structured and unstructured information. The meaning of the data is encoded in the graph in a natural language-like representation, making it easy to query and explore. Finally, knowledge graphs are easy to extend over time and to refine as new information arrives.  
In particular, once a knowledge graph is built with verified data, it is possible to extract facts from other sources with the techniques discussed above (like Named Entity Extraction and Triples Extraction) and to check them with the graph, that represents the truth, to understand if those sentences are generated by non-human user or they do not correspond to the open data facts.

The last part of the architecture is where all the processed data are visualized in a web dashboard. Based on the types of data processed in the pipeline, the views available to the final users will be different. In a use case where the data sources are articles and social media posts, some of the views would be:
- Statistical analysis of the amount of articles/posts divided by topic, sentiment, time, etc
- Named Entity Recognition related view  
- Latent Space of Entities, Articles and Open Data

The frameworks used for Named Entity Recognition are: NLTK and SpaCy. The first tool will be used also for the LDA technique in the topic extraction phase, along with Gensim.

For Text Similarity the main technique will be BERT, or Bidirectional Encoder Representations from Transformers, that is a new method of pre-training language representations which obtains state-of-the-art results on a wide array of Natural Language Processing (NLP) tasks. In particular, they trained a general-purpose “language under- standing” model on a large text corpus like Wikipedia so that the model is capable to be used for a lot of NLP tasks.

For the knowledge graph building phase, it is possible to extract triples with the Named Entity Recognition module discussed above from openly available sources (like DBpedia) to build an initial graph, that is then enriched with emerging knowledge from social media platforms or news sources. Finally, it can be queried to extract all the relationships between the elements.

## Run it on your machine

  

### Managing Dependencies

  

For managing dependencies in our project, we use <a href="https://python-poetry.org/">Poetry</a>.
Once installed, simply navigate to the project folder and run:

```bash
poetry install
```
  

### Installing and Running the tool

Before running this script, is necessary to set personalized configuration parameters at:
```bash
configuration/configuration.yaml
```
All the code is orchestrated by the *news_post_process.py* script placed in the root of project folder. 
It is possible to select which modules to run over the collected data, simply by commenting out the corresponding call in *process_doc* function of *news_post_process.py*.
  

## License

  

The content of this project is licensed under the [MIT license](LICENSE.md).
