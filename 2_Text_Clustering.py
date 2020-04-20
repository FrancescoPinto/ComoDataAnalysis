# Databricks notebook source
###################DATA PREPARATION######################
from pyspark.sql.functions import *

#load data
df = spark.read.format("csv").option("header", "true").option("multiline",'true').option("inferSchema", 'true').load("dbfs:/FileStore/tables/AllQuery3Tweets2017.csv")
df = df.dropna()
display(df)
df_with_date = df.withColumn("timestamp",from_unixtime(unix_timestamp('timestamp', 'HH:mm - dd MM yyyy')).cast("date")) 
#puoi anche usare "timestamp" per conservare anche l'ora, ma per ora non ti serve

#language augmentation
from polyglot.detect import Detector

df_lan = df_with_date.rdd.map(lambda x: (x["id"],Detector(u""+x["clean_text\r"],quiet = True).languages[0].name)).toDF(['id2','language'])

#join the two dataframes
df_with_date_and_language = df_with_date.join(df_lan, df_with_date.id == df_lan.id2).drop(df_lan.id2)
display(df_with_date_and_language)

#prepare ArrayType(StringType)for hashtags
temp_df= df_with_date_and_language.withColumn('hashtags',substring_index(col("hashtags"),"]",1))
temp_df= temp_df.withColumn('hashtags',substring_index(col("hashtags"),"[",-1))
temp_df= temp_df.withColumn('hashtags',split(col("hashtags"),","))
display(temp_df)
clean_df = temp_df

# COMMAND ----------

###################DATA PREPARATION######################
from pyspark.sql.functions import *

#load data
df2017 = spark.read.format("csv").option("header", "true").option("multiline",'true').option("inferSchema", 'true').load("dbfs:/FileStore/tables/Query_2017.csv")
df2017 = df2017.dropna()
df2017 = df2017.withColumnRenamed("clean_text", "clean_text\r")
df_with_date2017 = df2017.withColumn("timestamp",from_unixtime(unix_timestamp('timestamp', 'HH:mm - dd MM yyyy')).cast("date")) #puoi anche usare "timestamp" per conservare anche l'ora, ma per ora non ti serve

#language augmentation
from polyglot.detect import Detector

df_lan2017 = df_with_date2017.rdd.map(lambda x: (x["id"],Detector(u""+x["clean_text\r"],quiet = True).languages[0].name)).toDF(['id2','language'])

#join the two dataframes
df_with_date_and_language2017 = df_with_date2017.join(df_lan2017, df_with_date2017.id == df_lan2017.id2).drop(df_lan2017.id2)
display(df_with_date_and_language2017)

#prepare ArrayType(StringType)for hashtags
temp_df2017= df_with_date_and_language2017.withColumn('hashtags',substring_index(col("hashtags"),"]",1))
temp_df2017= temp_df2017.withColumn('hashtags',substring_index(col("hashtags"),"[",-1))
temp_df2017= temp_df2017.withColumn('hashtags',split(col("hashtags"),","))
display(temp_df2017)
clean_df2017 = temp_df2017

# COMMAND ----------

from pyspark.sql.types import ArrayType, FloatType

num_topics = 5

# COMMAND ----------

english_tweets = clean_df.filter(clean_df.language == "English")
italian_tweets = clean_df.filter(clean_df.language == "Italian")
un_tweets = clean_df.filter(clean_df.language == "un")
english_tweets2017 = clean_df2017.filter(clean_df2017.language == "English")
italian_tweets2017 = clean_df2017.filter(clean_df2017.language == "Italian")
un_tweets2017 = clean_df2017.filter(clean_df2017.language == "un")


# COMMAND ----------

english_tweets = english_tweets.drop("clean_text\r")
italian_tweets = italian_tweets.drop("clean_text\r")
un_tweets = un_tweets.drop("clean_text\r")

display(english_tweets)

# COMMAND ----------

import re as re
sqlContext.registerFunction("clean_tweet_1", lambda x: ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", x).split()))            
sqlContext.registerDataFrameAsTable(english_tweets,"english_tweets")
sqlContext.registerDataFrameAsTable(italian_tweets,"italian_tweets")
sqlContext.registerDataFrameAsTable(un_tweets,"un_tweets")

english_tweets = sqlContext.sql("SELECT *, clean_tweet_1(text) as clean_text from english_tweets")
italian_tweets = sqlContext.sql("SELECT *, clean_tweet_1(text) as clean_text from italian_tweets")
un_tweets = sqlContext.sql("SELECT *, clean_tweet_1(text) as clean_text from un_tweets")

display(italian_tweets)

# COMMAND ----------

print(english_tweets.count())
print(italian_tweets.count())
print(un_tweets.count())

# COMMAND ----------

def getStopWordsRemover(language):
  if language == "english":
    en_stopWords = StopWordsRemover().loadDefaultStopWords("english")
    en_stopWords.extend([u"lake",u"como",u"italy", u"co"])
    return (StopWordsRemover()
                .setInputCol("tokens")
                .setOutputCol("stopWordFree")
                .setStopWords(en_stopWords))
  elif language == "italian" or language == "un":
    it_stopWords = StopWordsRemover().loadDefaultStopWords("italian")
    it_stopWords.extend([u"lago",u"como",u"italia",u"co"])
    return (StopWordsRemover()
           .setInputCol("tokens")
           .setOutputCol("stopWordFree")
           .setStopWords(it_stopWords))
    
  

# COMMAND ----------

 def printTopics(lda_model, vocabArray):
  topicIndices = lda_model.describeTopics(maxTermsPerTopic = 10)
  wordNumbers = 10
  def topic_render(topic):
    terms = topic.termIndices
    result = []
    for i in range(wordNumbers):
      term = vocabArray[terms[i]]
      result.append(term)
    return result
  topics_final = topicIndices.rdd.map(lambda topic: topic_render(topic)).collect()
  for topic in range(len(topics_final)):
      print ("Topic" + str(topic) + ":")
      for term in topics_final[topic]:
          print (term)
      print ('\n')

# COMMAND ----------

#backup
'''from pyspark.ml.feature import RegexTokenizer, Tokenizer
from pyspark.ml.feature import CountVectorizer , IDF, StopWordsRemover
from pyspark.ml.linalg import Vector, Vectors
from pyspark.ml.clustering import LDA, LDAModel
from pyspark.ml.feature import NGram,HashingTF
from pyspark.ml import Pipeline

def prepareIntermediate(tweets,language,ngramsize):
#tknzr = TweetTokenizer() #forse è inutile visto che hai fatto un migliore clean_text!
#tokenizer = Tokenizer(inputCol="clean_text\r", outputCol="tokens")
  tokenizer = (RegexTokenizer()
               .setInputCol("clean_text")
               .setOutputCol("tokens")
                .setPattern("\\W+")
                .setToLowercase(True)
              .setMinTokenLength(2))

  #en_remover = (StopWordsRemover().setInputCol("tokens")      .setOutputCol("stopWordFree"))
  remover =getStopWordsRemover(language)
  #aggiungere uno stemmer???
  tokens = tokenizer.transform(tweets)
  clean_tokens = remover.transform(tokens)
  ngram = NGram(n=ngramsize, inputCol = "stopWordFree", outputCol = "ngrams")
  counts = CountVectorizer(inputCol = "ngrams", outputCol="raw_features", vocabSize = 10000, minDF = 2.0) 
  #1000, 3.0 dà risultati decenti, ma molto confusi
  #10000,2.0 dà discreti risultati
  #
  idf = IDF(inputCol="raw_features",outputCol="features")
  
  intermediate  = ngram.transform(clean_tokens)
  int_m = counts.fit(intermediate)
  intermediate = int_m.transform(intermediate)
  int_m = idf.fit(intermediate)
  intermediate = int_m.transform(intermediate)
  
  return intermediate, ngram, counts, idf,clean_tokens

def performClustering(tweets, language, ngramsize, num_topics):
 
  intermediate,ngram,counts,idf,clean_tokens = prepareIntermediate(tweets, language,ngramsize)
  max_iterations = 10 #30,100
  lda = LDA(k=num_topics, seed=1, optimizer="em")
  #en_pipeline = Pipeline().setStages([ngram, counts,lda]) 
  pipeline = Pipeline().setStages([ngram,counts,idf,lda]) 
  pipeline_model = pipeline.fit(clean_tokens)
  transformed = pipeline_model.transform(clean_tokens)
  #display(transformed)
  #pipeline_model.stages[3].logLikelihood()
  transformed.createOrReplaceTempView("transformed")
  
  return pipeline_model, transformed, intermediate'''

# COMMAND ----------

#from nltk.tokenize import TweetTokenizer
from pyspark.ml.feature import RegexTokenizer, Tokenizer
from pyspark.ml.feature import CountVectorizer , IDF, StopWordsRemover
from pyspark.ml.linalg import Vector, Vectors
from pyspark.ml.clustering import LDA, LDAModel
from pyspark.ml.feature import NGram,HashingTF
from pyspark.ml import Pipeline

def prepareIntermediate(tweets,language,ngramsize):
#tknzr = TweetTokenizer() #forse è inutile visto che hai fatto un migliore clean_text!
#tokenizer = Tokenizer(inputCol="clean_text\r", outputCol="tokens")
  tokenizer = (RegexTokenizer()
               .setInputCol("clean_text")
               .setOutputCol("tokens")
                .setPattern("\\W+")
                .setToLowercase(True)
              .setMinTokenLength(2))

  #en_remover = (StopWordsRemover().setInputCol("tokens")      .setOutputCol("stopWordFree"))
  remover =getStopWordsRemover(language)
  #aggiungere uno stemmer???
  tokens = tokenizer.transform(tweets)
  clean_tokens = remover.transform(tokens)
  ngram = NGram(n=ngramsize, inputCol = "stopWordFree", outputCol = "ngrams")
  counts = CountVectorizer(inputCol = "ngrams", outputCol="raw_features", vocabSize = 10000, minDF = 2.0) 
  #1000, 3.0 dà risultati decenti, ma molto confusi
  #10000,2.0 dà discreti risultati
  #
  idf = IDF(inputCol="raw_features",outputCol="features")
  
  intermediate  = ngram.transform(clean_tokens)
  int_m = counts.fit(intermediate)
  intermediate = int_m.transform(intermediate)
  int_m = idf.fit(intermediate)
  intermediate = int_m.transform(intermediate)
  
  return intermediate, ngram, counts, idf,clean_tokens

def performClustering(tweets_en, tweets_it,tweets_un, ngramsize, num_topics):
 
  intermediate_en,ngram,counts,idf,clean_tokens_en = prepareIntermediate(tweets_en, "english",ngramsize)
  intermediate_it, _,_,_,clean_tokens_it = prepareIntermediate(tweets_it, "italian",ngramsize)
  intermediate_un, _,_,_, clean_tokens_un = prepareIntermediate(tweets_un,"un",ngramsize)
  clean_tokens = clean_tokens_en.union(clean_tokens_it).union(clean_tokens_un)
  intermediate = intermediate_en.union(intermediate_it).union(intermediate_un)
  max_iterations = 10 #30,100
  lda = LDA(k=num_topics, seed=1, optimizer="em")
  #en_pipeline = Pipeline().setStages([ngram, counts,lda]) 
  pipeline = Pipeline().setStages([ngram,counts,idf,lda]) 
  pipeline_model = pipeline.fit(clean_tokens)
  transformed = pipeline_model.transform(clean_tokens)
  #display(transformed)
  #pipeline_model.stages[3].logLikelihood()
  transformed.createOrReplaceTempView("transformed")
  
  return pipeline_model, transformed, intermediate



# COMMAND ----------

#PCA
from pyspark.ml.feature import PCA 
from pyspark.ml.feature import StandardScaler

#gioca tra raw_features e features per vedere cosa va meglio
def performPCA(estrai_topic):
  #normalizzazione prima della PCA
  scaler = StandardScaler(inputCol="raw_features", outputCol="scaledFeatures",withStd=True, withMean=False)
  scalerModel = scaler.fit(estrai_topic)
  scaledData = scalerModel.transform(estrai_topic)

  pca = PCA(k=2, inputCol = "raw_features", outputCol = "pca")
  #NON HO CAPITO SE DEVI USARE RAW_FEATURES O MENO ...
  model = pca.fit(scaledData)
  transformed_df = model.transform(scaledData)
  print(model.explainedVariance)
  #display(transformed_df)
  return transformed_df

# COMMAND ----------

import numpy as np
import operator
def topTopicExtraction(transformed_tweets, num_topics):
  tentativo_estrai_topic = transformed_tweets.select("topicDistribution").collect()

  dati = {}
  j = 0
  for row in tentativo_estrai_topic:
    array_dict = {}
    for i in range(0,num_topics):
        array_dict[i] = row.topicDistribution[i]
    dati[j] =  array_dict
    j+=1  

  top_topic_per_post = []
  for k in dati:
    top_topic_per_post.append(sorted(dati[k].items(), key=operator.itemgetter(1), reverse = True))
    #row = sorted(row.items()[0].items(), key = operator.itemgetter(1))
  print(top_topic_per_post)      
  #print(dati)
  #lista_dati_topics = [-np.sort(-row.topicDistribution.toArray()) for row in tentativo_estrai_topic]
  #print(lista_dati_topics)

# COMMAND ----------

from pyspark.mllib.linalg import Vectors, VectorUDT
from pyspark.sql.types import FloatType

def get_top_k_topics_idx(topicDistribution,k,num_topics):
  array_dict = {}
  for i in range(0,num_topics):
    array_dict[i] = topicDistribution[i]
  temporary = sorted(array_dict.items(), key=operator.itemgetter(1), reverse = True)[0:k]
  #print(temporary)
  idxes = []
  for _ in range(0,k):
    idxes.append(temporary[_][0])
  return Vectors.dense(idxes)

def get_top_1_topics_idx(topicDistribution,k,num_topics):
  array_dict = {}
  for i in range(0,num_topics):
    array_dict[i] = topicDistribution[i]
  temporary = sorted(array_dict.items(), key=operator.itemgetter(1), reverse = True)[0:k]
  #print(temporary)
  idxes = []
  for _ in range(0,k):
    idxes.append(temporary[_][0])
  return Vectors.dense(idxes)[0]

k = 3
sqlContext.registerFunction("get_top_k_topics_idx",udf(lambda x: get_top_k_topics_idx(x,k,num_topics), VectorUDT()))  
sqlContext.registerFunction("get_top_1_topics_idx",udf(lambda x: float(get_top_1_topics_idx(x,1,num_topics)), FloatType()))  

#la seguente non è necessaria ...
#sqlContext.registerFunction("get_top_k_topics_weights", udf(lambda x: get_top_k_topics_weights(x,k,num_topics),VectorUDT())) 


# COMMAND ----------

def extractTopics(transformed):
  transformed.createOrReplaceTempView("transformed")
  estrai_topic = sqlContext.sql("select *, get_top_k_topics_idx(topicDistribution) as topTopics,get_top_1_topics_idx(topicDistribution) as firstTopic  from transformed")
  #, get_top_k_topics_1(topicDistribution) as topTopics
  display(estrai_topic)
  estrai_topic.createOrReplaceTempView("extracted_transformed")
  return estrai_topic

# COMMAND ----------

import matplotlib.pyplot as plt
colors = [ (1.0, 0.0, 0.0), (0.0,1.0,0.0),  (0.0, 0.2,0.0),
             (1.0, 1.0, 0.0),  (1.0, 0.0, 1.0),  (0.0, 1.0, 1.0),  (0.0, 0.0, 0.0), 
             (0.5, 0.5, 0.5),  (0.8, 0.8, 0.0), (0.3, 0.3, 0.3)]
def plotClusters(first_principal_component, second_principal_component,num_topcs):
  fig, axes = plt.subplots()
  for a in range(0,num_topcs):
    axes.plot(first_principal_component[a],second_principal_component[a],"o",color = colors[a])
  display(fig)

# COMMAND ----------

def extractPCAByTopic(transformed_df,num_topic):
  first_principal_component = []
  second_principal_component = []
  for _ in range(0,num_topic):
    temp = transformed_df.filter("firstTopic = "+str(_))
    first_principal_component.append([row.pca[0] for row in temp.collect()])
    second_principal_component.append([row.pca[1] for row in temp.collect()])
    print(temp.count())
  return first_principal_component, second_principal_component


# COMMAND ----------

def show_cluster_percentages(transformed_df):
  total = transformed_df.count()
  counts_per_cluster = []
  counts_per_cluster =transformed_df.select("firstTopic").groupBy("firstTopic").count().orderBy("firstTopic")
  display(counts_per_cluster.withColumn("perc", col("count")/total))
#fai un pieplot se necessario

# COMMAND ----------

def show_sentiment_percentages(sentiment_df):
  total = sentiment_df.count()
  counts_per_cluster = []
  counts_per_cluster =sentiment_df.select("firstTopic").groupBy("firstTopic").count().orderBy("firstTopic")
  display(counts_per_cluster.withColumn("perc", col("count")/total))

# COMMAND ----------

#extract prototypes (avg of cluster, baricenter = bar)
def extract_prototypes(transformed_df,num_topics_chosen):
  def vec_to_array(v):
    print(v)
    return list([float(x) for x in v])

  num_topics_chosen = transformed_df.select("firstTopic").distinct().count()
  transformPCA = udf(vec_to_array, ArrayType(FloatType()))
  temp1 = transformed_df.withColumn("pca", transformPCA("pca"))
  #display(temp1)
  agg_pca = temp1.agg(array(*[avg(col("pca")[i]) for i in range(0,2)])).collect()[0][0]
  #display(agg_pca)
  list_by_topic = []
  for i in range(0,num_topics_chosen):
    list_by_topic.append(temp1.select("pca").where("firstTopic = "+ str(i))) 
  list_of_bar = []
  for i in range(0,num_topics_chosen):
    pca_of_topic = list_by_topic[i].agg(array(*[avg(col("pca")[_]) for _ in range(0,2)])).alias("bar")
    list_of_bar.append(pca_of_topic)
  clean_bar = []
  for p in range(0,num_topics_chosen):
    clean_bar.append(list_of_bar[p].collect()[0][0])
  #print(clean_bar)
  fig, axes = plt.subplots()
  print(num_topics_chosen, clean_bar)
  for a in range(0,num_topics_chosen):
    axes.plot(clean_bar[a][0],clean_bar[a][1],"^",color = colors[a],label=str(a))
    axes.legend(bbox_to_anchor=(0., 1.02, 1., .102), loc=3,
           ncol=num_topics_chosen, mode="expand", borderaxespad=0.)
  axes.plot(agg_pca[0],agg_pca[1],"o",color = colors[0])
  display(fig)

# COMMAND ----------

from pyspark.ml.pipeline import PipelineModel
vocforsave = 10000
mindf_forsave = 2
def saveModels(models):
  for g in ngram_sizes:
    for t in num_topics:
      models[g][t-2].save("PipelineLDA_ALL_LANGUAGES_"+str(g)+"_"+str(t)+"_"+str(vocforsave)+"_"+str(mindf_forsave))
      #TIENI LA SEGUENTE RIGA! DOCUMENTA I NOMI DEI VECCHI MODELLI!
      #models[g][t-2].save("PipelineLDA_"+str(g)+"_"+str(t)+"_"+str(vocforsave)+"_"+str(mindf_forsave))
      #print("saving "+str(g)+" "+str(t))

def loadModels():
  models = {1:[],2:[],3:[]}
  for g in ngram_sizes:
    for t in num_topics:
      models[g].append(PipelineModel.load("PipelineLDA_ALL_LANGUAGES_"+str(g)+"_"+str(t)+"_"+str(vocforsave)+"_"+str(mindf_forsave)))
      #PRESERVA RIGA SUCCESSIVA! DOCUMENTA NOMI VECCHI MODELLI! (ancora salvati)
      #models[g].append(PipelineModel.load("PipelineLDA_"+str(g)+"_"+str(t)+"_"+str(vocforsave)+"_"+str(mindf_forsave)))
      #print("loading "+str(g)+" "+str(t))
  return models


# COMMAND ----------

#backup
'''ngram_sizes = [1,2,3]
num_topics = [2,3,4,5,6,7,8,9,10]
logLikelihood = {1:[],2:[],3:[]}
logPerplexity = {1:[],2:[],3:[]}
en_pipeline_models = {1:[],2:[],3:[]}
en_tweets_transformeds = {1:[],2:[],3:[]}
do_computations = True
if do_computations == True:
  for g in ngram_sizes:
    for t in num_topics:
      en_pipeline_model, en_tweets_transformed, en_intermediate = performClustering(english_tweets,"english",g,t)   
      ll = en_pipeline_model.stages[3].logLikelihood(en_intermediate)
      lp = en_pipeline_model.stages[3].logPerplexity(en_intermediate)
      logLikelihood[g].append(ll)
      logPerplexity[g].append(lp)
      en_pipeline_models[g].append(en_pipeline_model)
      en_tweets_transformeds[g].append(en_tweets_transformed)
else:
  en_pipeline_models = loadModels()
  for g in ngram_sizes:
    for t in num_topics:
      en_intermediate,ngram,counts,idf,clean_tokens = prepareIntermediate(english_tweets, "english",g)
      en_tweets_transformeds[g].append(en_pipeline_models[g][t-2].transform(clean_tokens))
      ll = en_pipeline_models[g][t-2].stages[3].logLikelihood(en_intermediate)
      lp = en_pipeline_models[g][t-2].stages[3].logPerplexity(en_intermediate)
      logLikelihood[g].append(ll)
      logPerplexity[g].append(lp)'''

# COMMAND ----------

english_tweets.printSchema()
italian_tweets.printSchema()
un_tweets.printSchema()

# COMMAND ----------

ngram_sizes = [1,2,3]
num_topics = [2,3,4,5,6,7,8,9,10,11,12,13]
logLikelihood = {1:[],2:[],3:[]}
logPerplexity = {1:[],2:[],3:[]}
pipeline_models = {1:[],2:[],3:[]}
tweets_transformeds = {1:[],2:[],3:[]}
do_computations = False
if do_computations == True:
  for g in ngram_sizes:
    for t in num_topics:
      pipeline_model, tweets_transformed, intermediate = performClustering(english_tweets,italian_tweets,un_tweets,g,t)   
      ll = pipeline_model.stages[3].logLikelihood(intermediate)
      lp = pipeline_model.stages[3].logPerplexity(intermediate)
      logLikelihood[g].append(ll)
      logPerplexity[g].append(lp)
      pipeline_models[g].append(pipeline_model)
      tweets_transformeds[g].append(tweets_transformed)
else:
  pipeline_models = loadModels()
  for g in ngram_sizes:
    for t in num_topics:
      intermediate_en,ngram,counts,idf,clean_tokens_en = prepareIntermediate(english_tweets, "english",g)
      intermediate_it, _,_,_,clean_tokens_it = prepareIntermediate(italian_tweets, "italian",g)
      intermediate_un, _,_,_, clean_tokens_un = prepareIntermediate(un_tweets,"un",g)
      clean_tokens = clean_tokens_en.union(clean_tokens_it).union(clean_tokens_un)
      intermediate = intermediate_en.union(intermediate_it).union(intermediate_un)
      clean_tokens.cache()
      intermediate.cache()
      tweets_transformeds[g].append(pipeline_models[g][t-2].transform(clean_tokens))
      ll = pipeline_models[g][t-2].stages[3].logLikelihood(intermediate)
      lp = pipeline_models[g][t-2].stages[3].logPerplexity(intermediate)
      logLikelihood[g].append(ll)
      logPerplexity[g].append(lp)

# COMMAND ----------

'''print("Learned topics (as distributions over vocab of " + str(en_pipeline_models[1][8].stages[3].vocabSize())
      + " words):")
topics = en_pipeline_models[1][8].stages[3].topicsMatrix()
#print(topics)
for topic in range(10):
    print("Topic " + str(topic) + ":")
    for word in range(en_pipeline_models[1][8].stages[3].vocabSize()):
        print(" " + str(topics[word,topic]) + ", "+en_pipeline_models[1][8].stages[1].vocabulary[word])#[word]))#[topic]))'''

# COMMAND ----------

if do_computations:
  saveModels(pipeline_models)

# COMMAND ----------

import matplotlib.pyplot as plt
fig, axes = plt.subplots(1,2)
axes[0].plot(num_topics,logLikelihood[1], color = "red")
axes[0].plot(num_topics,logLikelihood[2], color = "green")
axes[0].plot(num_topics,logLikelihood[3], color = "blue")
axes[1].plot(num_topics,logPerplexity[1], color = "red")
axes[1].plot(num_topics,logPerplexity[2], color = "green")
axes[1].plot(num_topics,logPerplexity[3], color = "blue")
display(fig)
#lowest perplexity = best model
#but keep in mind we are measuring it on the TRAINING data ... optimistic!
#in teoria, si potrebbe fare un dataset e splittarlo in training e test ...
#il problema è che non posso assicurare in nessun modo che lo splitting non vada a cancellare dei topic dal training/test set ... quindi meglio andare di ispezione dei topic finali 

# COMMAND ----------

def printMyTopics(g):
  for t in num_topics:
    print("=======================================================================")
    print("========== NGRAM = "+str(g)+", NTOPICS = "+str(t)+" ======================")
    printTopics(pipeline_models[g][t-2].stages[3], pipeline_models[g][t-2].stages[1].vocabulary)
    print("=======================================================================")
    print("===========================FINE========================================")

# COMMAND ----------

#num_topics = [2,3,4,5,6,7,8,9]
g = 1
printMyTopics(g)


# COMMAND ----------

g = 2
printMyTopics(g)

# COMMAND ----------

g = 3
printMyTopics(g)


# COMMAND ----------

#3.2 -> pochi topics
#3.3 -> volta/este/montagne + concorso eleganza/tattoedchef/lago + villaeste eleganza/tosetticomo nuova collezione sposi
#3.4 -> tattoedchef/cernobbio/volta + tosetti/monte + concorso eleganza lovecars/catslover + ilpomodorino/cucina italiana/tattoos/duomo
#3.5 DISCRETO -> biagio antonacci a cernobbio/stadio sinigaglia/via alessandro volta + tattoedchef/mountain biking/ilpodorinocomo cibo italiano free wifi + lago/catslover/piazza/lungolago + tosetti nuovo catalogo sposa + concorso eleganza villa este/monte boletto 
#3.6 DISCRETO -> lago/san fermo/santo stefano/teatro sociale/centro commerciale + catslover/tatoos/biagiolive + villa este cernobbio/volta/monte boletto/monti + catalogo sposi/tattoedchof + mountain biking/piazza duomo/gite/architecture + concorso eleganza villa este/sciarada/san giovanni
#3.7 -> città dei balocchi/centro/sheraton/san fermo/brunate + villaeste lovecars/catslover/centro commerciale +  volta/este/tattoos/beautifulday + villa este cernobbio/duomo/teatro sociale/architecture + montagne/biagio antonacci + tattoedchef/tattos/stadio/mountain biking + sposi/vegan
#3.8 BUONO -> lago/centro/photo + centro commerciale/architecture/mountain biking/food + sposi/tattoo + via volta/biagiolive/stadio sinigaglia + montagne + piatti tipici/free wifi/saporidautunno/villa este + tattoedchef/catslove/duomo + concorso eleganza villa este/teatro sociale/sheraton 
#3.9 -> tattoedchef/san giovanni/teatro sociale/sheraton/villa erba/chef gasparini/piarata + volta/bellagio/tattos/este + sposi + catslover/gita fuoriporta/seagulls + lago/città dei balocchi/pizza/gente notte/biagiolive + girl roman tattoo/villa este/faro voltiano + cucina italiana/ilpodmodorino/montagne + villadeste eleganza/centro commerciale/villa flori/repost/saporidautunno + concorso eleganza/lungolago/mountain biking/sciarada/ospedale
#3.10 OTTIMO (a parte uno ridondante)-> stadio sinigaglia/tattoo/centro commerciale/hotel villa filori + sposi + montagne/gite/sheraton + eleganza villa este/lungolago/lago/biagiolive + san fermo battaglia/vegan/italyfood/saporidautunno/ilpomodoringo + volta/cittàdeibalocchi/tattoos/sciarada/jazz food wine/ospedale villa aprica + villa este/san giovanni/cernobbio/architecture/oldtown + tattoedchef/catslover + lovecars/concorso eleganza/il pomodorino + piazza duomo/santo stefano/centro/photo/farovoltiano/memorial 
#3.11 -> villadeste lovecars/ piazza santostefano sanfedele/beautiful day + concorso eleganza villa este/ stadio sinigaglia/ san fermo/alessandro volta + villa este cernobbio/teatro sociale/mojiteria/gente notte lombardia/faro voltiano + bellagio/città dei balocchi/ig/finished running + biagiolive/mountain biking/san giovanni + lungolag/ilpomodorino/cibo italiano/wifi + tattoedchef/volta/montagne + catslover/como pic/piazza duomo + ig/lakecomo/glamour + photo/repost app/tattoo/architecture + sposi 
#3.12 -> sposa/boletto + phto villa/lungolago/sheraton/tattoos volta + villadeste lovecars/tattoos/ig + tattoedchef/teatro sociale/architecture + lago/concorso eleganza/centro commerciale/duomo + san giovani/cittàdeibalocchi/beautifulday/romantic sunset + catslover/sciarada/pub grill pizza + piazza/bellagio/chihuahua/piazza/repost + villa este cernobbio/sheraton/comopic/jazz food wine + ilpomodorino/biagiolive/free wifi + volta/faro/tatuaggi/pirata + san fermo battaglia/mountain biking/villa este cernobbio/pub città
#3.13 -> ilpomodoinocomo/centro commerciale/wifi/cittàbalocchi + tattooedchef/catslover/san fermo + italy/lake/hotel villa fiori/ilpomodorino + sposi/boletto + sinigaglia/tattoo volta/mycat + volta/tattoos/lungolago/cernobbio + oldtown/architecture/gite/seagulls + san giovanni/faro volta/brunate/landscapes + villa este cernobbio/mountain biking/sciarada/duomo + sheraton/sky beautifulday/romantic sunset+ concorso eleganza villadeste lovecars/vacances holidays + teatro sociale/chef daniele gasparini/violin/brunocanino/ialian food + biagiolive2016/artphotography/casa del fascio/ospedale vilal aprica

# COMMAND ----------

#2.2 -> pochi
#2.3 -> pochi
#2.4 -> pochi
#2.5 -> pochi
#2.6 -> tattoedchef/volta/città balocchi/lakecomo/photo + villa erba/concorso eleganza/villa este + catslover/photo duomo/sheraton + sciarada/teatro sociale/cernobbio + lungolago/finished running endomondo endorphins/sciarada + wizardxp studios/olmo/tosetti
#2.7 -> confuso
#2.8 -> no
#2.9 -> meglio
#2.10 -> è relativamente buono! però accoppia gli argomenti (ogni cluster è su 2 argomenti), però gli argomenti sono ben divisi 
#2.11 BUONO -> concorso eleganza + tosetti sposi + volta/tattoos + wizardxp + tattoedchef/centro commerciale + biagiolive/sheraton + sciarada + catslover/stadio sport + brunate funicolare/ilpomodorino italyfood + villa olmo/lungolago/lago + teatro sociale/made club/milan/ig/night 
#2.12 BUONO -> castiglioni/volta + brunate/lago + sposi + sciarada/made club/hipster + catslover/città balocchi +  duomo/cernobbio/biagiolive + teatro sociale/volta/social + montano lucino/centro commerciale +  sinigaglia/villa erba/tattoo/faro + wizardxp/piazza/twitter/photo +  concorso eleganza/sheraton + lago/villa este/cucina italiana
#2.13 BUONO -> lago/duomo/sport + volta/faro/tattoos + sciarada/san fermo/cernobbio + sposi + brunate/bear bearded/lago + lungolago/teatrosociale/cittàbalocchi/biagiolive + villa olmo/tattooed chef/brunate +  concorso eleganza/faro brunate/duomo + wizardxp/sheraton/lago + montano lucino/tempio volta/piazza cavour + piattiitaliani + animalslover + villa este/cernobbio chef

# COMMAND ----------

#1,8 non è male (ma è molto rumoroso visto che è 1)
#1,9 >> 1,8
#1,10 -> nature/beautiful/cernobbio/holiday/photo + brunate/lunch/food/sunday/aperitivo/funicolare/vintage + sciarada/villa/tattoo/mojiteria/concorso/piazza + tattoedchef/wizardxp/ig + lombardy/repost + running/mountain biking/natale/bellagio + cernobbio/art/orticolario/love + altro ... 


# COMMAND ----------

#mia idea di clustering dopo parecchi esperimenti
#volta/tattoo/piscina
#lago/foto/lungolago/ville/architecture
#brunate,summer,vacanza
#dinner,lunch,food,jazz,wine, pizza, sciarada
#sport (running, monti, mountain bike, endomondo endorfine)
#enogastronomia tattoedchef, il pirata, centro commerciale
#teatro sociale, duomo, piazza
#villaeste, wedding, cars, fashion
#lovecats,animalslover
#pics, photos, landscapes, beautiful
#ios, apple, studio

# COMMAND ----------

#1.2 -> villa.lago.cernobbio.italia + tattoo.lago.brunatete.
#1.3 -> brunate.lago.dinner.tatto ... eccc. troppo confuso e banale
#1.4 -> ancora troppo vago e confuso
#1.5 -> same
#1.6 -> schifo
#troppo rumore anche dopo

# COMMAND ----------

#2.2 -> lago/lungolago/wizardxp/tatooedchef + villa este cernobbio/volta/finished running 
#2.3 -> villa este/lungolago/volta/montano lucino + lungolago/morning/catslover/finished running + endomondo endorphins/lago/tattoedchef/piazza 
#2.4 -> volta/catslover/animals + villa este cernobbio/montano lucino/italien + lago/lungolago/endomondo endorphins/duomo + wizardxp/san giovanni/roman helmet 
#2.5 -> alessandro volta/duomo + villa este/erba/cernobbio/duomo +  running/endomondo endorphins/mountain biking + catslover/montano lucino (pirata)/tattoedchef + piscina sinigaglia/monti/lungolago
#2.6 -> tattoedchef/lago/porto + lungolago/catslover/san giovanni + endomondo endorphins/finished running/villa olmo/este + lungolago cernobbio travel san fermo aston martin villa este + lago/monti/wizardxp + volta/duomo/clooney/tattoos
#2.7 -> volta/catslover + san giovanni/tattoo tatuaggi/volta lighthouse/wizardxp + villa/finished running/piazza duomo/tatto + hotel villa/lago/lungolago/teatro sociale/centro + tattoedchef/centro commerciale/finished mountain biking + last night/villa este/monte + villa erba/olmo/cernobbio/pub grill 
#2.8 -> catslover + endomndo endorphins/finished running/mountain biking/villa este + lago/lungolago/san fermo/breakfast/porto/bellagio + villa erba/este/brunate/vanquish zagato/aston martin + tattoedchef/last night/duomo/george clooney/brunate + villa olmo/san giovanni/lungolago/tattoo + sheraton/lagodicomo/montano lucino/villadeste lovecars + volta/monti 
#2.9 -> villa este/piscina sinigaglia/brunate/lungolago + photo/porto/lago + villa erba/endomondo endorphins/piazza duomo/finished mountain biking + montano lucino/lungolago/roman helmet tattoo/funicolare/centro commerciale +  san giovanni/lagodicomo/monti + villa olmo/catslover/brunate + finished running/lakecomo/concorso eleganza/aston martin + lungolago/tattoedchef/hotel villa/architecture + posted photo/alessandro volta/sheraton/tattoos
#2.10 -> volta/roman helmet girl tattoo/brunate + wizardxp/montano lucino/lagodicomo/cernobbio/architecture + tattoedchef/posted/piazza duomo/san giovanni + tempio voltiano/lago/duomo/villa erba/cernobbio/tattoo + villa este cernobbio/good morning/ mojiteria/teatro sociale + villa erba/montagne + lungolago/funicolare/cernobbio/traveling + running/endomondo endorphins/mountain biking, villadeste lovecars/chihuahua + villa este/centro commerciale/aston martin/san fermo/tattoo/concorso eleganza/vanquish zagato + catslover

# COMMAND ----------

#3.2 -> photo lago/villa/catslover/lungolago/tattoedchef castiglioni + villa este/volta/piazza/photo/monte boletto
#3.3 -> piazza/tattoo/duomo/architecture + lago/centro commerciale/volta/sheraton/villadeste lovecars fcarphoto + catslover/monte boletto/tattooedchef/villa este cernobbio
#3.4 -> mountain biking/volta/oldtwon/architecture/sciarada/vacances + catslover/monte boletto/montagne + lago/stazione san giovanni/sheraton/teatro sociale + villa este cernobbio/piazza/roman helmet tattoo/tattoedchef .../centro commerciale
#3.5 -> tatto roman helmet, bellagio, lungolago, jazz food wine + monte boletto/mountain biking/centro commerciale/teatro sociale + villa este cernobbio/piazza/mojiteria sciarada/san giovanni/pub grill pizza/centro + catslover/tattoos via alessandro/piazza duomo + lago/tattoedchef .../cernobbio/holidays/hotel villa
#3.6 -> lago/tattoedchef/mountain biking/centro commerciale/cernobbio + lago/stazione san giovanni/bellagio/architecture/oldtown summer + lungolago/duomo/sciarada/sheraton/centro commerciale la grande/brunate + volta/catslover/monte boletto + villa este cernobbio/piazza duomo/teatro sociale/ chiuahua/san fermo + girl roman helmet tattoo/pub grill pizza/vacances
#3.7 -> architecture/sheraton/oldtown summer + piazza/lungolago/lovecars villadeste/piazza duomo/centro commerciale/san fermo della battaglia + montagna/monte boletto/iw2obx + volta/catslover/tattoos + roman helmet tattoo/teatro sociale/sciarada mojiteria/brunate/bistrot + photo lago/tattoedchef/san giovanni/centro commerciale/jazz food wine + lago/villa este/duomo/mountain biking/chihuahua/piarata montano lucino
#3.8ABBASTANZABUONO -> villa este cernobbio/tattoedchef/mountain biking/sheraton/bellagio/hotel/villa/centro commerciale + photo lago/lungolago/villa/cathedral/stazione san giovanni/casa del fascio/villa olmo + piazza/holidays/vacances/alessandro volta/picoftheday + teatro sociale/grand hotel villa/finished running/pirata montano lucino/tattoos/teatro + catslover/architecture + tattoo/pub grill pizza/vintage jazz food wine + volta/catslover/tattos/villadeste lovecars/glamour vip + montagne varie 
#3.9 BUONO -> lungolago/tattoedchef/glamour vip + catslover/mountain biking/lago/centro/holidays + piazza duomo/sciarada/osteria del beuc/cardamomo persian palace + villa este/teatro sociale/chiuahua/albero di momo + glirl roman helmet tattoo/alessandro volta/città varie + centro commerciale/bellagio/sheraton/centro commerciale la grande/brunate bistrot + montagne varie + pub grill pizza/pirata montano lucino/landscapes + architecture/oldtown summer/casa del fascio/hotel villa filori
#3.10 -> villa/lovecats/vintage jazz food wine + pirata montano lucino/grand hotel villa/concorso eleganza/aston martin vanquish zagato + pub grill pizza/boatig sailing summer/lighthouse brunate + piazza duomo/landscapes/chihuahua + villadeste lovecars/birrificio/corfidiosinner/città + lago/finished mountain biking/sheraton/brunate + volta/tattos/commerciale la grande grandate/holiday cernobbio + teatro sociale/san fermo della battaglia/san giovanni/candelaria + montagne/architecture + lungolago/tattoedchef/tattoo/sciarada

# COMMAND ----------

#NO 3,2 -> villa este/lago italia/mountain biking/architecture + volta/catslover/tattoedchef/tattos  NO
#NO 3,3 -> photos piazza/villa/duomo/architecture/sheraton + villa este/catslover/volta/tattoo + boletto/monte/teatro sociale   NO
#FORSE 3,4 -> volta/tattos è piazza/villa este cernobbio/monteboletto/castiglionigastronomia + architecture/sheraton/lungolago + catslover/centro commerciale/teatro sociale
#FORSE 3,5 -> photo villa/lago/piazza/tattoo/sciarada/grill pizza + monte boletto/mountain biking/architecture + brunate/duomo/jazz food/landscapes + volta/lungolago/tattos/holidays + villaestecernobbio/catslover/castiglionigastronomia/teatro sociale/centro commerciale
#FORSE+3,6 -> volta/centro commerciale/tattos + villa este cernobbio/duomo/lovecars/fermo della battaglia + tatto/san giovanni/holidays/vacances + catslover/tattoedchef/monteboletto + lungolago/architecture/sheraton/teatro solciale di + concorso eleganza/auto/dinner/pirata montano lucino
#3,7 -> volta/oldtown/architecture/montano licino/tattos + sheraton/lago/cittàvarie + tattoedchef castiglioni/monte boletto/teatro sociale + catslover/brunate + villa este/duomo/centro comerciale/pub grill/ironman + girl roman helmet tatto/mountain biking/bellagio/sciarada/sangiovanni + pictures 
#3,8 -> photo sheraton/lago/brunate/pub grill + catslover/pirata montano lucino + volta/teatro sociale/tattos vittoria/holiday + italia/villa/lago/cernobbio/sciarada + tattoedchef castiglioni/centro commerciale/piazza domo/ig + villa este cernobbio/architettura/piazza duomo/ironman + jazz food vintage/lovecars/landscapes pictures + roman helmet tattoo/monte boletto 
#FORSE+3,9 -> piazza/tattoedchef/duomo/città/ospedale + architecture/sheraton/centro/oldtown summer/diner + catslover/vacances/cars + villa este/cernobbio/pirata montano licino/concorso eleganza + tatto/pub grill pizza/commerciale + alessandro volta/pictures + monti/chihuahua + lago/duomo/jazz food wine/bar + lago/lungolago/teatro sociale/sciarada/fermo della battaglia/san giovanni 
#3,10 -> catslover/architecture + villaeste/lago/piratamontanolucino + teatro sociale/tattoo/pub grill pizza+ lago/voltiano/lipomo + tattoedchef castiglioni/mountain biking/centro commerciale/vacances + sciarada/villaeste lovecars/food wine jazz/sheraton + duomo/chihuahua/piazza + lago/landscape/pictures/fermo della battaglia  + volta/monte boletto +  lungolago/cittàvarie/villaerba

# COMMAND ----------


#2,2 -> villa este/olmo/erba/lungolago + catslover/running/twitter posted 
#2,3 -> lungolago/sheraton/villaeste/tattoo/romangirlhelmet + lungolago/villaerba/catslover/olmo/montanolucino + este cernobbio/lago/italia/running/duomo
#2,4 -> lungolago/montano lucino/ios/villaolmoerba/tattoedchefcastiglioni + running/lungolaglo/tattoo/sheraton + lago/volta/endomondoendorphins/catslover + villaeste/piazzaduomo/sangiovanni/lago 
#2,5 -> vilaeste/erba/montanoLucino/duomo/tattoedchef castiglionienoteca ecc. +  italia/lago/running/ednomondo endoprhins/lago/monteboletto + lungolago/catslover+ alessandro volta/tattoo + ios 10/brunate/san giovanni/beautiful 
#2,6 -< volta/este/beautiful/lucino/lungolago/tattoo + tattoedchef castiglionieno/duomo/lungolago + duomo ... troppo confuso
#2,7 -> catslover/montano lucino/helmet tattoo girl + lungolago/running/endomondo endorphins/castiglioni + villa este/lago/cernobbio/hotel/george clooney/teatro sociale + architettura/piazza/vanquish zagato/old town + volta/olmo/tattoos + lago/morning/duomo/brunate/life electric + lungolago/monti 
#2,8 -> lago/roman helmet tattoo/sheraton + wizardxp/lungolago/brunate/tattoo/hotel villa/piazza + lungolago/san giovanni/made club/teatro sociale + monte boletto/ios + alessandro volta/finished running/endomondo endorphins/tattoos + cernobbio/lago/duomo/holidays + villa erba/catslover/faro di volta + villaeste/tattoedchef castiglioni/mojiteria/centro commerciale/vanquish zagato <-------------------------------- questo non è male
#2,9 -> este/duomo/lungolago/tattoo + montano lucino/sheraton/tattoedchef/centro commerciale + tatoo/mojiteria/faro di volta/montagna + volta/tattos/cernobbio + catslover/villa erba/pub grill + running/endomondo endorphins/san giovanni/porto/hotel villa/finished mountain + villa este/lago/last night/san fermo/milan + lago/tempio voltiano/villa/chihuahua + birrivico/vintage jazz/wizardxp/food wine 
#2,10 -> volta/tattos/mojiteria + piazza/brunate/funicolare + piazza duomo/duomo/bellagio/ironman/chihuahua + faro/tempio voltiano/casa mia/villa este/san giovanni/cat/automobili + endorfine/running/mountain biking + tattoedchef castiglioni/pub grill pizzeria/diner + villa olmo/montano lucino/centro commerciale/teatro sociale/cernobbio + catslover/hotel villa/tattoo + lago/monti 

# COMMAND ----------

#PROBLEMA GENERALE: tanto rumore da lago/lungolago ecc. <- ma non posso rimuoverlo, altrimenti perdo i post turistici <- quindi non seguo troppo il ranking delle frequenze e faccio leva sulle differenze per costruire i cluster

# COMMAND ----------

#1,2 -> photo/tattoo/lungolago/lago + lago/cernobbio/photo/villa ...meeeeh
#1,3 -> tattoo/travel/lakecomo/morning/dinner + villa este/cernobbio/lago/lunch + summer/day/brunate/lago ... meeeeeh
#1,4 -> italia/lago/summer + villaeste/lunch+ brunate/dinner/photo/lago+ tattoo/lakecomo ... meeeeeeh
#1,5 -> dinner/lunch/food/piazza + tattoo/sciarada/volta/apple + lakecomo/cernobbio/summer/travel + brunate/villa/lago/foto + foto/love/flowers/villa/landscape/friends -> meglio, ma meh
#1,6 -> brunate/villa/lunch/love/bar + lago/city/amazing + morning/coffee/villa/work + dinner/breakfast/architecture/lakecomo/summer/stazione + posted/apple/catslover + tattoo/volta/cernobbio
#1,7 -> sciarada/travel/cernobbio/lago/holiday + beautiful, architecture/endomondo  ecc. ecc.
#INUTILE SCRIVERE TUTTO, perché ngram_size = 1 produce risultati troppo confusi 
#Il meno confuso pare essere 1,10 ... ma comunque dà alcuni topic rindondanti e che si basano su vocaboli poco significativi


# COMMAND ----------

#2,2 -> no
#2,3 -> no
#2,4 -> no
#2,5 -> alessandrovolta/piazzaduomo/wizardxpstudios + villaeste/villaerba/cernobbio/lungolago/beautiful/duomo + finished running/endomondo endorphins/mountain biking + cats/montanolucino/animals/tattooedchef castiglionigrastronomia + piscina sinigaglia/monte boletto/hotel villa ###################QUESTO MI SEMBRA BUONO
#2,6 -> alessandro volta/catlovers/animalslovers + wizardxp studios/stazione san giovanni/tattoo/george clooney/faro di volta + villa/finished running/roman helmet tattoo/piazza + ios, hotel villa, teatro sociale, milano + tattoedchef castiglionigastronomia/endomondo endorphins/brunate/centro commerciale/finished mountain + villa este/casa/last night/cernobbio villa este + lago/villa erba/cernobbio/pub grill/beautiful place/villa olmo<- è confuso
##########<- manca 2,7 (riesegui)
#2,8 -> cats/animals/casamia/via durini + running/endomondo endorphins/este cernobbio/wizardxp studios/tattoos/mountain biking + lago/lungolago/san fermo/colazione/bellagio + villa erba/este/cernobbio/funicolare brunate/vanquish zagato/aston martin + tatooedchef castiglioni.../last night/piazza duomo/george clooney/bruante + villa olmo/san giovanni/tatoo helmet + sheraton hotel/beautiful place/lago/lungolago/montano lucno/lovecars + volta/boletto/monte
#2,9 -> este/cernobbio/piscina sinigaglia/brunate/lungolago + lago/casa/italia/porto + endomondo endorphins/piazza duomo/life electric/finished mountain + montano lucino/lungolago/roman helmet/funicolare/centro commerciale + san giovanni/monte boletto/iw2obx + villa olmo/catslover/animals + finished running/concorso eleganza/aston martin/milano + hotel villa/lungolago/architettura/tattoedchef + wizardxp studios/alessandro volta/sheraton/tatoos/george clooney/villadeste lovecars
#2.10 -> alessandro volta/tatoo/brunate + wizardxp/montano lucino/lago/architettura/oldtown/pub grill + photo/video/ios/piazza duomo/castiglioni .../san giovanni/duomo + tempio voltiano/duomo/life electric/milano/villa erba/cernobbio/tatoo + good morning/villa este cernobbio/ mojiteria/teatro sociale + monte boletto/villa erba + lungolago/funicolare/cernobbio/travel/vacances + finished running/endomondo endorphins/mountain biking/finished mountains/villadeste lovecars + /villad'este/aston martin/concorso eleganza/san ferno/vanquish zagato + catslover/animals/casa ################# questo non è male


# COMMAND ----------

#ngram ntopics si/no gruppi
#1,2 -> no, non discerne nulla di utile
#1,3 -> ""
#1,4 -> ""
#1,5 -> inizia ad emergere qualcosa: foto/video/turismo + villa/cernobbio/este/hotel + brunate/duomo/lago + lago/cena/architettura/bellezza + sciarada/endomondo/running
#1,6 -> "" : milano/photo/hotel + sciarada/villa/breakfast/beautiful + tattoo/school/flowers + matrimonio/cena/brunate/endomondo/night + summer/lago/sunday + villadeste/family/travel + villa/cernobbio/catslover
#1,7 -> : hotel/milano/lombardy/iw2obx + sciarada/villa/breakfast + tattoo/school/flowers + wedding/brunate/night/endomondo + lunch/summer/lagodicomo/sunday + travel/italia/apple/villadeste/family + villa/cernobbio/friends/pizza/catslover

#1,8 -> brunate/duomo/summer/catslover/lago/trip+ travel/villadeste/lago/holiday +tattoo/cernobbio/girl/tatuaggi/school + breakfast/lago/work/hotel/villa/wedding + wizardxp/studios + dinner/pizza/endomondo/running/mountain/casa + lunch/sunday/food/stazione + sciarada/milano
#1,9 -> milano/lunch/coffee + apple/tatuaggi/casa + summer/catslover/love + sciarada/work/sun/ios/relax + dinner/landscape/villa/faro + lago/colazione/rose/architettura/bellagio/natura + viaggio/villa/view/italian/cernobbio + lago/twitter/lungolago/sunday + wedding/endomondo/running/brunate/erba
#1,10 -> landscape/cats/animals/lighthouse +  dinner/sharada/villadeste/club/casa + villa/lunch/duomo/breakfast/cernobbio/lago + night/mountains/birthday/wedding/lake + brunate/funicolare/lunch + tattoo/endomndo/volta/running + hotel/work/pizza/selfie/sunset/wizardxp + pittureperfect/cathedral/lakecomo/wedding + life/nature/garden/lakecomo/piazza + summer/architecture/travel/weekend/milan

# COMMAND ----------

#ATTENZIONE: per un po' faremo finta che la LDA ritorni un partizionamento dei dati in base ai topic (ciò non è vero, ma sarà utile per una breve analisi preliminare)
num_topc = 12-2
estrai_topic = extractTopics(tweets_transformeds[2][num_topc]) #num_topic = topic - 2 ... ricorda lo scarto nell'array

# COMMAND ----------

transformed_df = performPCA(estrai_topic)
#dai pca scores, visualizzare il tutto è praticamente isignificante, viene fatto per esercizio

# COMMAND ----------

first_principal_component, second_principal_component = extractPCAByTopic(transformed_df,num_topc)

# COMMAND ----------

plotClusters(first_principal_component, second_principal_component,num_topc)

# COMMAND ----------

show_cluster_percentages(transformed_df)

# COMMAND ----------

from polyglot.downloader import downloader
downloader.download("embeddings2.en")
downloader.download("ner2.en")
downloader.download("sentiment2.en")
downloader.download("embeddings2.it")
downloader.download("ner2.it")
downloader.download("sentiment2.it")

# COMMAND ----------

#qualche test preliminare
from polyglot.text import Text
text = Text(u":( che stronzi che sono ... non pensavo si potesse essere così bastardi","it") #non riconosce "infami", "stronzi" e altre amenità ... né le faccine ... non sarà proprio la sentiment analysis più accurata di questo mondo (ma di dataset gratuiti non ne ho trovati in italiano)
for w in text.words:
  print(u"{:<16}{:>2}".format(w, w.polarity))
print("###########################################################")
text = Text(u"^^ dal profondo del mio cuore pace amore e allegre felicitazioni :) la gioia del mondo sprofonda nel nostro cuore. bellissimo il concerto di biagio antonacci","it") #non riconosce alcune parole anche qui, neanche le faccine ... senza contare che per lui "di" è positivo 
for w in text.words:
  print(u"{:<16}{:>2}".format(w, w.polarity))
print("###########################################################")  
text = Text("The movie but even though Barack Obama went mad after him, Hideous little bastard, suck my dick. I hated to state that he was not as good as we supposed.","en")
#l'inglese si comporta MOLTO meglio dell'italiano 
for w in text.words:
  print(u"{:<16}{:>2}".format(w, w.polarity))
#first_entity = text.entities[0] <- riconosce troppe poche entità per ottenere qualcosa di significativo 
#print(first_entity)
#print(first_entity.positive_sentiment)
#print(first_entity.negative_sentiment)
#usa questo per arricchire il dataset prima della visualizzazione
print("###########################################################")  
text = Text("I'm so glad you had a child! he is so sweet and mild. You couldn't hope for a better child!","en")
#l'inglese si comporta MOLTO meglio dell'italiano, anche se fa un'analisi di polarità più che di sentimento (per avere l'analisi di sentimento si dovrebbero usare le entities)
for w in text.words:
  print(u"{:<16}{:>2}".format(w, w.polarity))

# COMMAND ----------

#insoddisfacente parecchio ... però per l'italiano mi è sembrata l'unica alternativa (VISTO CHE NON HO DEI DATASET DECENTI!)
def get_polyglot_polarity(v,lan):
#calcoli la media sulle parole non nulle (se normalizzassimo i risultati otterremo spari positivi/negativi ... cosa poco realistica; se considerassimo i neutri otterremmo un forte bias sullo 0 ... e non vogliamo)
#so che è un modo molto brutale e rozzo di farlo, e ci sarà un po' di rumore ... ma incrociamo le dita
  if lan == "English":
    language = "en"
  elif lan == "Italian":
    language = "it"
  else: #un
    language = "it"

  text = Text(v,language)
  count = 0
  summation = 0
  for w in text.words:
    if w.polarity != 0:
      summation = summation + w.polarity
      count += 1
  
  if count == 0:
    return 0
  else:
    return float(summation)/float(count)
  
polyglot_sentiment_udf = udf(get_polyglot_polarity,FloatType())
sentiment_polyglot_df = transformed_df.withColumn("sentiment",polyglot_sentiment_udf("clean_text","language"))

# COMMAND ----------

display(sentiment_polyglot_df) #risultato abbastanza indecente ... troppo polarizzato

# COMMAND ----------

from textblob import TextBlob 
#TEXTBLOB funziona bene, ma solo con l'inglese
def get_textblob_sentiment(v):
  return TextBlob(v).sentiment.polarity

textblob_sentiment_udf = udf(get_textblob_sentiment, FloatType())
sentiment_df = transformed_df.withColumn("sentiment", textblob_sentiment_udf("clean_text"))

display(sentiment_df)

# COMMAND ----------

#idea, usiamo un sentiment analyzer ibrido ... è il meglio che posso provare a fare senza un dataset decente in italiano (purtroppo sono a pagamento)
def get_hybrid_sentiment(v,lan):
#calcoli la media sulle parole non nulle (se normalizzassimo i risultati otterremo spari positivi/negativi ... cosa poco realistica; se considerassimo i neutri otterremmo un forte bias sullo 0 ... e non vogliamo)
#so che è un modo molto brutale e rozzo di farlo, e ci sarà un po' di rumore ... ma incrociamo le dita
  if lan == "English":
    #language = "en"
    return TextBlob(v).sentiment.polarity
  elif lan == "Italian" or "un":
    language = "it"
    text = Text(v,language)
    count = 0
    summation = 0
    for w in text.words:
      if w.polarity != 0:
        summation = summation + w.polarity
        count += 1

    if count == 0:
      return 0.
    else:
      return float(summation)/float(count)
  
hybrid_sentiment_udf = udf(get_hybrid_sentiment,FloatType())
tempp = transformed_df.filter("clean_text != ''") #ci sono 9 post in arabo con testo vuoto che creano problemi ... non ci riguardano proprio, ragion per cui li scarto
sentiment_hybrid_df = tempp.withColumn("sentiment",hybrid_sentiment_udf("clean_text","language"))
sentiment_hybrid_df = sentiment_hybrid_df.drop("tokens","stopWordFree","ngrams","raw_features","features","scaledFeatures","pca")

# COMMAND ----------

display(sentiment_hybrid_df)
#questo grafico non ha molto senso, perché assume che l'LDA faccia assegnamenti flat... ma in realtà dà una distribuzione dei topic ... però è giusto per vedere se tutto è andato come doveva
#in ogni caso, i risultati sui topic dal 2 al 4 sono più senzati di quello a 0 (che prende la maggioranza dei post, e quindi necessariamente avrà più bias)

# COMMAND ----------

display(sentiment_hybrid_df.filter("sentiment <=-0.40 and firstTopic = 0").select("text")) #beh, a parte un po' di rumore, non è male

# COMMAND ----------

#summation ...
display(sentiment_hybrid_df)

# COMMAND ----------

#avg
display(sentiment_hybrid_df)

# COMMAND ----------

sentiment_hybrid_df.createOrReplaceTempView("h_sentiment")
avg_daily = spark.sql("select timestamp,avg(likes),avg(num_replies),avg(reshares),avg(sentiment) from h_sentiment group by timestamp order by timestamp")
print("Correlation num_replies-sentiment:")
print(avg_daily.stat.corr("avg(num_replies)","avg(sentiment)"))
print("Correlation reshares-sentiment: ")
print(avg_daily.stat.corr("avg(reshares)","avg(sentiment)"))
print("Correlation likes-sentiment: ")
print(avg_daily.stat.corr("avg(likes)","avg(sentiment)"))
print("COME SI PUO' VEDERE SONO TUTTE CORRELAZIONI MOLTO DEBOLI ...")

# COMMAND ----------

sentiment_hybrid_df.select("sentiment").describe().show()

# COMMAND ----------

#consideriamo come outliers mean + 2*stddev (riduciamo un po')
total = sentiment_hybrid_df.count()
print("Tutti i tweet: "+str(total))
positive_tweets = sentiment_hybrid_df.filter("sentiment >0.97 ").select("timestamp","creator","clean_text")
print("Numero di tweet eccezionalmente positivi (i.e. > 0.97):")
print(str(positive_tweets.count()) + " " + str(float(positive_tweets.count())/float(total))+"%") #cioè circa un 10 di tutti i messaggi esaminati (circa 9000) sono straordinariamente positivi
print("Numero di tweet di sentimento > 0:")
pos = sentiment_hybrid_df.filter("sentiment >0 ").count()
print(str(pos) + " " + str(float(pos)/float(total))+"%") #circa il 30% sono positivi

negative_tweets = sentiment_hybrid_df.filter("sentiment < - 0.97 ").select("timestamp","creator","clean_text")
print("Numero di tweet eccezionalmente negativi (i.e. < - 0.97):")
print(str(negative_tweets.count()) + " " + str(float(negative_tweets.count())/float(total))+"%") #cioè circa il 5% straordinariamente negativi
print("Numero di tweet di sentimento < 0:")
neg = sentiment_hybrid_df.filter("sentiment < 0 ").count()
print(str(neg) + " " + str(float(neg)/float(total))+"%") #circa 8% negativi


# COMMAND ----------

clean_text_tweets_for_entities = clean_df.select("id","clean_text\r")
joined_sentiment = sentiment_hybrid_df.join(clean_text_tweets_for_entities,sentiment_hybrid_df.id == clean_text_tweets_for_entities.id)


# COMMAND ----------

from polyglot.text import Text
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import udf, struct
from pyspark.sql import functions as f


def extract_NN_by_lan(sent,lan):
  entities = []
  try:
    text = Text(sent,hint_language_code=lan)
    for sent in text.sentences:
      for entity in sent.entities:
        temp = ""
        for term in entity._collection:
          temp = temp+" " + term
        entities.append(temp)
        #print(vars(entity._collection))
        #print(entity.tag, temp)
    return entities
  except ValueError:
    return []


extract_entities_en = udf(lambda text: extract_NN_by_lan(text,"en") , ArrayType(StringType()))
extract_entities_it = udf(lambda text: extract_NN_by_lan(text,"it") , ArrayType(StringType()))

from pyspark.sql.functions import count,sum

def getEntitiesInMultipleDataframes(data1,data2):
  temp1 = getEntitiesInTweets(data1)
  temp2 = getEntitiesInTweets(data2)
  result = temp1.union(temp2)
  #result = result.groupBy("entities").agg(sum("count").alias("tot")).orderBy("tot",ascending = False)
  #display(result)
  result.show(result.count(),False)
  return result

def getEntitiesInTweets(data):
  temp = data
  temp = temp.select("entities")
  temp = temp.withColumn("entities",f.explode("entities"))
  #temp = temp.filter(f.length(col("entities")) >0)
  temp = temp.filter("length(entities)>0")
  temp = temp.groupBy("entities").agg(count("entities").alias("count")).orderBy("count", ascending = False)
  #display(temp)
  return temp

# COMMAND ----------

it_sent = joined_sentiment.filter("language = 'Italian'").filter("sentiment > 0.97")
en_sent = joined_sentiment.filter("language = 'English'").filter("sentiment > 0.97")
#un_sent =  joined_sentiment.filter("language = 'un'") non può agire sugli un ... 

en_tw_entities = en_sent.withColumn('entities',extract_entities_en(en_sent["clean_text\r"]))
it_tw_entities = it_sent.withColumn("entities",extract_entities_it(it_sent["clean_text\r"]))
#un_tw_entities = un_sent.withColumn("entities",extract_entities_it(it_sent["clean_text\r"]))

# COMMAND ----------

getEntitiesInMultipleDataframes(en_tw_entities,it_tw_entities)


# COMMAND ----------

it_sent = joined_sentiment.filter("language = 'Italian'").filter("sentiment <- 0.97")
en_sent = joined_sentiment.filter("language = 'English'").filter("sentiment <- 0.97")
#un_sent =  joined_sentiment.filter("language = 'un'") non può agire sugli un ... 

en_tw_entities = en_sent.withColumn('entities',extract_entities_en(en_sent["clean_text\r"]))
it_tw_entities = it_sent.withColumn("entities",extract_entities_it(it_sent["clean_text\r"]))
getEntitiesInMultipleDataframes(en_tw_entities,it_tw_entities)


# COMMAND ----------

display(joined_sentiment.filter("language = 'Italian'").filter("sentiment <- 0.97").select("clean_text"))
#c'è tanto rumore, ma spesso funziona ...

# COMMAND ----------

display(joined_sentiment.filter("language = 'English'").filter("sentiment <- 0.97").select("clean_text"))


# COMMAND ----------

display(joined_sentiment.filter("language = 'Italian'").filter("sentiment > 0.97").select("clean_text"))


# COMMAND ----------

display(joined_sentiment.filter("language = 'English'").filter("sentiment > 0.97").select("clean_text"))


# COMMAND ----------

def compute_weighted_sentiment(topicDistribution,sentiment): #<- prova a sostituirlo
  sentiment*topicDistribution
  weighted_sentiment = []
  for i in range(0,12):
    weighted_sentiment.append(topicDistribution[i]*sentiment)
  return Vectors.dense(weighted_sentiment)

joined_sentiment.createOrReplaceTempView('all_sentiment')
sqlContext.registerFunction("compute_weighted_sentiment",udf(lambda topicDistribution,sentiment: compute_weighted_sentiment(topicDistribution, sentiment), VectorUDT()))  


# COMMAND ----------

joined_sentiment.printSchema()

# COMMAND ----------

total = spark.sql('select timestamp, clean_text,text, compute_weighted_sentiment(topicDistribution,sentiment) as weighted,topicDistribution,sentiment,language,clean_text\r as simple from all_sentiment')

# COMMAND ----------

def getWeighted(weighted,index):
  return float(weighted[index])

getWeighted_udf = udf(getWeighted, FloatType())
for i in range(0,12):
  total = total.withColumn("topic_"+str(i),getWeighted_udf(total["weighted"],lit(i)))

total.printSchema()

# COMMAND ----------

display(total)

# COMMAND ----------

topics = list(range(0,12))
s_t = []
s_t_toplot = []
for _ in topics:
  s_t.append(total.select("timestamp","clean_text","topic_"+str(_)))
  s_t_toplot.append([x["topic_"+str(_)] for x in s_t[-1].collect()])

# COMMAND ----------

def plotBoxplot(data_to_plot):
  fig, axes = plt.subplots()
  #fig = plt.figure(1, figsize=(9, 6))
  #ax = fig.add_subplot(111)
  bp = axes.boxplot(data_to_plot, patch_artist=True)
  for flier in bp['fliers']:
      flier.set(marker='o', color='#e7298a', alpha=0.5)
  display(fig)
plotBoxplot(s_t_toplot)

# COMMAND ----------

list_up_bound = []
list_low_bound = []
for i in range(0,12):
  print("###################################")
  print("Topic "+str(i)+":")
  description = total.select("topic_"+str(i)).describe()
  col = [x["topic_"+str(i)] for x in description.collect()]
  list_up_bound.append(float(col[1])+2*float(col[2]))
  list_low_bound.append(float(col[1])-2*float(col[2]))
  description.show()


# COMMAND ----------

list_up_bound,list_low_bound

# COMMAND ----------

from pyspark.ml.fpm import FPGrowth
from pyspark.sql.types import ArrayType,StringType

def remove_duplicates_in_array(a):
  temp=[]
  for str in a:
    temp.append(str.lower())
  return list(set(temp))

def performAssociationMining(data):
  tokenizer = (RegexTokenizer()
               .setInputCol("clean_text")
               .setOutputCol("tokens")
                .setPattern("\\W+")
                .setToLowercase(True)
              .setMinTokenLength(2))
  #remover =getStopWordsRemover(language)
  tokens = tokenizer.transform(data)
  #clean_tokens = remover.transform(tokens)
  ngram = NGram(n=3, inputCol = "tokens", outputCol = "ngrams")
  preFPGrowth= ngram.transform(tokens)
  remove_duplicates = udf(remove_duplicates_in_array, ArrayType(StringType()))
  for_association = preFPGrowth.withColumn("clean_ngrams", remove_duplicates("ngrams"))
  fpGrowth = FPGrowth(itemsCol="clean_ngrams", minSupport=0.02, minConfidence=0.3)
  model = fpGrowth.fit(for_association)
  freqItemsets = model.freqItemsets.orderBy("freq",ascending = False)
  #display(freqItemsets)
 # oneItem = freqItemsets.filter(size(col("items"))==1)
  #twoItem = freqItemsets.filter(size(col("items"))==2)
  #threeItem = freqItemsets.filter(size(col("items"))==3)
  return freqItemsets#,oneItem,twoItem,threeItem

# COMMAND ----------

relevant_negative_itemsets = []
for i in range(0,12):
  freqItemsets = performAssociationMining(total.filter("topic_"+str(i)+" < "+str(list_low_bound[i])))
  relevant_negative_itemsets.append(freqItemsets)


# COMMAND ----------

#troppo rumore, non si capisce molto ... ho provato a guardare un po' tutti ... 
display(relevant_negative_itemsets[0].filter("size(items) == 2"))

# COMMAND ----------

total.printSchema()

# COMMAND ----------

display(total.filter("language = 'Italian'"))

# COMMAND ----------

def printANegativeEntities(idx):
  print("################################################")
  print("####TOPIC "+str(idx))
  all_sent = total.filter("topic_"+str(idx)+"<"+str(list_low_bound[idx]))
  all_tw_entities = all_sent.withColumn('entities',extract_entities_en(all_sent["text"]))
  result = getEntitiesInTweets(all_tw_entities)
  result.show(result.count(),False)
  return result

def printAPositiveEntities(idx):
  print("################################################")
  print("####TOPIC "+str(idx))
  all_sent = total.filter("topic_"+str(idx)+">"+str(list_up_bound[idx]))
  all_tw_entities = all_sent.withColumn('entities',extract_entities_en(all_sent["text"]))
  result = getEntitiesInTweets(all_tw_entities)
  result.show(result.count(),False)
  return result

# COMMAND ----------

all_negative_entities = []
for i in range(0,12):
  all_negative_entities.append(printANegativeEntities(i))

# COMMAND ----------

all_positive_entities = []
for i in range(0,12):
  all_positive_entities.append(printAPositiveEntities(i))

# COMMAND ----------

neg_diff = []
pos_diff = []
neg = []
pos = []

for i in range(0,12):
  all_negative_entities[i].createOrReplaceTempView("all_negative_entities_"+str(i))
  all_positive_entities[i].createOrReplaceTempView("all_positive_entities_"+str(i))
  neg_diff.append(spark.sql("select entities from all_negative_entities_"+str(i)+" where entities not in (select entities from all_positive_entities_"+str(i)+")"))
  pos_diff.append(spark.sql("select entities from all_positive_entities_"+str(i)+" where entities not in (select entities from all_negative_entities_"+str(i)+")"))
  neg.append([x.entities for x in neg_diff[i].collect()])
  pos.append([x.entities for x in pos_diff[i].collect()])
  

# COMMAND ----------

for i in range(0,12):
  print("######################")
  print("Topic "+str(i))
  print(neg[i])


# COMMAND ----------

neg_exclusive_per_topic = []
def join_other_neg(idx):
  temp = []
  for _ in range(0,12):
    if idx != _:
      temp.extend(neg[_])
  return temp

for i in range(0,12):
    neg_exclusive_per_topic.append([item for item in neg[i] if item not in join_other_neg(i)])
    print(neg_exclusive_per_topic[i])

# COMMAND ----------

def getNegItemsInAllLists():
  temp = neg[0]
  for item in temp:
    counter = 0
    for _ in range(1,12):
      if neg[_] == item:
        counter +=1
    if counter != 11:
      temp.remove(item)
  return temp

getNegItemsInAllLists()

# COMMAND ----------

for i in range(0,12):
  print("######################")
  print("Topic "+str(i))
  print(pos[i])


# COMMAND ----------

pos_exclusive_per_topic = []
def join_other_pos(idx):
  temp = []
  for _ in range(0,12):
    if idx != _:
      temp.extend(pos[_])
  return temp

for i in range(0,12):
    pos_exclusive_per_topic.append([item for item in pos[i] if item not in join_other_pos(i)])
    print(pos_exclusive_per_topic[i])

# COMMAND ----------

def getPosItemsInAllLists():
  temp = pos[0]
  for item in temp:
    counter = 0
    for _ in range(1,12):
      if pos[_] == item:
        counter +=1
    if counter != 11:
      temp.remove(item)
  return temp

getPosItemsInAllLists()

# COMMAND ----------


