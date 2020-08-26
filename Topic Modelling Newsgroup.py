# Databricks notebook source
from sklearn.datasets import fetch_20newsgroups

newsgroups_train = fetch_20newsgroups(subset="train")
newsgroups_test = fetch_20newsgroups(subset="test")

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %sh 
# MAGIC pip install nltk
# MAGIC pip install --upgrade pip
# MAGIC python -m nltk.downloader all

# COMMAND ----------

from nltk.stem import WordNetLemmatizer, PorterStemmer
import nltk
nltk.download('averaged_perceptron_tagger')
nltk.download('wordnet')

# COMMAND ----------

type(newsgroups_train)

# COMMAND ----------

import pandas as pd
df = pd.DataFrame([newsgroups_train.data, newsgroups_train.target.tolist()])
df

# COMMAND ----------

#
df = pd.DataFrame([newsgroups_train.data, newsgroups_train.target.tolist()]).T
df.columns = ['text', 'target']

targets = pd.DataFrame(newsgroups_train.target_names)
targets.columns=['title']

ngout = pd.merge(df, targets, left_on='target', right_index=True)

display(ngout)

# COMMAND ----------

## Converting pandas dataframe into spark dataframe
sdf = spark.createDataFrame(ngout)
display(sdf)

# COMMAND ----------

from pyspark.sql.functions import split
from pyspark.sql.functions import monotonically_increasing_id, col

# COMMAND ----------

sdf = sdf.withColumn("text_sep", split(sdf.text, "\n\n")).select(col('text'), col('target'), col('title'), col('text_sep').getItem(1), 
                                                                col('text_sep').getItem(2)).withColumn("id", monotonically_increasing_id())

display(sdf)

# COMMAND ----------

##
sdf.printSchema()

# COMMAND ----------

## Creating a table
temp_table_name = "newsgroup"
sdf.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from newsgroup

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from newsgroup where `text_sep[2]` is null

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from newsgroup where `text_sep[1]` =''

# COMMAND ----------

from pyspark.sql.types import FloatType
import re


# COMMAND ----------

def clean_text(in_string):
  remove_email = re.sub('\S*@\S*\s?', '', in_string)
  remove_nl = re.sub('\s+', ' ', remove_email)
  remove_othr = re.sub("\'|\>|\:|\-", "", remove_nl)
  return remove_othr

## Spark will run in a distributed mode, 
spark.udf.register("clean", clean_text)

# COMMAND ----------

# MAGIC %sql
# MAGIC select clean(CASE when `text_sep[2]` is null then `text_sep[1]` when `text_sep[1]`='' then `text_sep[2]` else
# MAGIC CONCAT(`text_sep[1]`, ' ', `text_sep[2]`) END), target, title, id FROM newsgroup where `text_sep[2]` is not null and `text_sep[1]` <> ''

# COMMAND ----------

sdf = spark.sql("select clean(CASE when `text_sep[2]` is null then `text_sep[1]` when `text_sep[1]`='' then `text_sep[2]` else \
CONCAT(`text_sep[1]`, ' ', `text_sep[2]`) END) as text, target, title, id FROM newsgroup where `text_sep[2]` is not null and `text_sep[1]` <> ''")

display(sdf)

# COMMAND ----------

sdf.count()

# COMMAND ----------

from pyspark.sql.functions import col, length
display(sdf.where(length(col('text')) < 100))

# COMMAND ----------

##
sdf = sdf.where(length(col('text')) > 100)

# COMMAND ----------

##
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover
tokenizer = RegexTokenizer(inputCol="text", outputCol="tokens", pattern="\\W+", minTokenLength=4, toLowercase=True)
tokenized = tokenizer.transform(sdf)

# COMMAND ----------

display(tokenized)

# COMMAND ----------

##
spremover = StopWordsRemover(inputCol="tokens", outputCol="spfiltered")
spremoved = spremover.transform(tokenized)

display(spremoved.select("tokens", "spfiltered"))

# COMMAND ----------

##
porter = PorterStemmer() ## Stemming
lemma = WordNetLemmatizer() ## Lemmatization
def word_tokenize(text):
  # print(text)
  pos = nltk.pos_tag(text)
  final = [lemma.lemmatize(word[0]) if (lemma.lemmatize(word[0]).endswith('e', 'ion') or len(word[0]) < 4) else
          porter.stem(word[0]) for word in pos]
  return final

# COMMAND ----------

spremoved.printSchema()

# COMMAND ----------

# spremoved.withColumn("stemmed", [tokenize(words) for words in spremoved])
stemmed = spremoved.rdd.map(lambda tup: (tup[1], tup[2], tup[3], word_tokenize(tup[5])))
# tup[1] is target
# tup[2] is title
# tup[3] is id
# tup[5] is spfiltered

# COMMAND ----------

stemmed.collect()

# COMMAND ----------

##
news_df = stemmed.toDF(schema=['target', 'title', 'id', 'word'])
display(news_df)

# COMMAND ----------


