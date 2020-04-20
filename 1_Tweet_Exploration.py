# Databricks notebook source


# COMMAND ----------

import matplotlib.pyplot as plt
dbutils.fs.ls("dbfs:/FileStore/tables/")

# COMMAND ----------

#load data
df = spark.read.format("csv").option("header", "true").option("multiline",'true').option("inferSchema", 'true').load("dbfs:/FileStore/tables/AllQuery3Tweets2017.csv")
df.cache()
display(df)


# COMMAND ----------

print("Il numero totale di post del 2016 è: " + str(df.count()))


# COMMAND ----------

temp = df = df.dropna()
print(temp.count())
#ci sono giusto un paio di tweet che producono unN/A (infatti prima abbiamo vistoche erano 10366, ora sono 10364 ... trascurabilissimi)

# COMMAND ----------

df = df.na.fill("")
#get Date instead of String for Timestamp
from pyspark.sql.functions import *
df_with_date = df.withColumn("timestamp",from_unixtime(unix_timestamp('timestamp', 'HH:mm - dd MM yyyy')).cast("date")) #puoi anche usare "timestamp" per conservare anche l'ora, ma per ora non ti serve
display(df_with_date)

# COMMAND ----------

#language augmentation
from polyglot.detect import Detector

df_lan = df_with_date.rdd.map(lambda x: (x["id"],Detector(u""+x["clean_text\r"],quiet = True).languages[0].name)).toDF(['id2','language'])

#join the two dataframes
df_with_date_and_language = df_with_date.join(df_lan, df_with_date.id == df_lan.id2).drop(df_lan.id2)
display(df_with_date_and_language)



# COMMAND ----------

#language check with full text
df_lan_to_check = df_with_date.rdd.map(lambda x: (x["id"],x["text"],Detector(u""+x["text"],quiet = True).languages[0].name)).toDF(['id2','language'])

#join the two dataframes
join_to_check = df_lan.join(df_lan_to_check, df_lan_to_check.id2 == df_lan.id2).drop(df_lan.id2)
display(join_to_check)

#tipicamente comunque migliori prestazioni usando clean_text ... procediamo con clean_text

# COMMAND ----------

#prepare ArrayType(StringType)for hashtags
temp_df= df_with_date_and_language.withColumn('hashtags',substring_index(col("hashtags"),"]",1))
temp_df= temp_df.withColumn('hashtags',substring_index(col("hashtags"),"[",-1))
temp_df= temp_df.withColumn('hashtags',regexp_replace(col("hashtags"),"[']",""))
temp_df= temp_df.withColumn('hashtags',regexp_replace(col("hashtags"),"[ ]",""))
temp_df= temp_df.withColumn('hashtags',split(col("hashtags"),","))
display(temp_df)
clean_df = temp_df



# COMMAND ----------

#inspect the Schema and publish SQL views
clean_df.printSchema()
clean_df.createOrReplaceTempView("tweets")
timestamp_df = clean_df.select("timestamp").distinct()
timestamp_df.createOrReplaceTempView("timestamps")


# COMMAND ----------

from pyspark.sql.types import FloatType
#convenience function for month counts
def count_by_month_df(query_type, language):
  query = ""
  if query_type == "count_all":
    query = '''
            SELECT month(timestamp) as month, COUNT(id) as countResult
            FROM tweets
            GROUP BY month
            ORDER BY month ''' #WHERE timestamp >= cast('2016-06-01' as date)
  elif query_type == "English":
    query = '''
            SELECT month(timestamp) as month, COUNT(id) as countResult, language
            FROM tweets
            WHERE language =
            'English'
            GROUP BY month,language
            ORDER BY month ''' #timestamp >= cast('2016-06-01' as date) and 
  elif query_type == "Italian":
    query = '''
            SELECT month(timestamp) as month, COUNT(id) as countResult, language
            FROM tweets
            WHERE language =
            'Italian'
            GROUP BY month,language
            ORDER BY month ''' #timestamp >= cast('2016-06-01' as date) and
  elif query_type == "count_all_by_language":
    query = '''
            SELECT month(timestamp) as month, language, COUNT(id) as countResult
            FROM tweets
            GROUP BY month, language
            ORDER BY month ''' #            WHERE timestamp >= cast('2016-06-01' as date)

  elif query_type == "count_num_languages":
    query = '''Select month, count(language) as countResult from lanMonths where language <> "un" group by month'''
  elif query_type == "specified_language":
    query = "select month(timestamp) as month, COUNT(*) as countResult FROM tweets where language = '"+language+"' GROUP BY month ORDER BY month"
  elif query_type == "un":
    query = "select month(timestamp) as month, COUNT(id) as countResult, language FROM tweets WHERE language = 'un' GROUP BY month,language ORDER BY month"
  month_hist_df = spark.sql(query)
  month_hist_df = month_hist_df.filter(col('month').isNotNull())
  #cast per dopo, altrimenti era long e creava problemi
  month_hist_df = month_hist_df.withColumn("countResult", month_hist_df["countResult"].cast(FloatType()))
  if language == "":
    display(month_hist_df)
  return month_hist_df

# COMMAND ----------

#count for each month and for each language the number of tweets
lan_time_hist_df = count_by_month_df("count_all_by_language","")
lan_time_hist_df.createOrReplaceTempView("lanMonths")


# COMMAND ----------

#conta la varietà di linguaggi 
lan_month_count = count_by_month_df("count_num_languages","")
display(lan_month_count)


# COMMAND ----------

from pyspark.sql import functions as f

all_lang_for_inter = spark.sql('''
            SELECT distinct language
            FROM tweets
             ''')
all_lang_for_inter.count() #108 linguaggi distinti

# COMMAND ----------

list_of_not_present = []
for index in range(1,13):
  month_languages = spark.sql('''
            SELECT distinct language
            FROM tweets
            WHERE month(timestamp) = '''+str(index))
  temporary = [ r.language for r in all_lang_for_inter.subtract(month_languages).agg(f.collect_set('language').alias('language')).collect()]
  list_of_not_present.append(temporary)

# COMMAND ----------

for index in range(0,12):
  print("Missing from month "+str(index+1) + ":\n"+ str(list_of_not_present[index]))
#queste lingue minori potrebbero anche essere dovute a "rumore" nella classificazione, o semplicemente al fatto che le persone di quella popolazione usano altre lingue per comunicare su twitter e quindi i dati in lingua su twitter sono pochi

# COMMAND ----------

#fai il rank per ogni mese dei linguaggi (per ispezione preliminare) 
ranked_lan_month_count = spark.sql('''select month, language, countResult from lanMonths order by month,countResult desc''')
display(ranked_lan_month_count)


# COMMAND ----------

total_per_month = count_by_month_df("count_all","")
total_per_month = total_per_month.withColumn("language",lit("all"))
display(total_per_month)#.union(en_per_month).union(it_per_month))


# COMMAND ----------

#conta tutti i post per ogni mese, e raccogli i dati in due liste
month_list = [row.month for row in total_per_month.collect()]
total_messages_per_month = [row.countResult for row in total_per_month.collect()]
en_per_month = count_by_month_df("English","")
it_per_month = count_by_month_df("Italian","")
un_per_month = count_by_month_df("un","")
display(total_per_month.union(en_per_month).union(it_per_month).union(un_per_month))



# COMMAND ----------

#crea una lista contenente i ranking dei linguaggi per ciascun mese
rank_by_month = []
rank_size = 5
for i in range(0,len(month_list)):
  query = "select language, countResult*100/"+str(total_messages_per_month[i])+ " as perc from lanMonths where month = "+ str(month_list[i])+ " order by countResult desc " 
  rank_by_month.append(spark.sql(query).take(rank_size))
  
#display(rank_by_month[0])
    
  

# COMMAND ----------

display(rank_by_month[0])

# COMMAND ----------

display(rank_by_month[1])

# COMMAND ----------

display(rank_by_month[2])

# COMMAND ----------

display(rank_by_month[3])

# COMMAND ----------

display(rank_by_month[4])

# COMMAND ----------

display(rank_by_month[5])

# COMMAND ----------

display(rank_by_month[6])

# COMMAND ----------

display(rank_by_month[7])

# COMMAND ----------

display(rank_by_month[8])

# COMMAND ----------

display(rank_by_month[9])

# COMMAND ----------

display(rank_by_month[10])

# COMMAND ----------

display(rank_by_month[11])

# COMMAND ----------

#arabic, italian, spanish, english, corsican, french, german, turkish, russian
#plot their behavior temporally
it = [r.countResult for r in count_by_month_df("specified_language","Italian").collect()]
ar = [r.countResult for r in count_by_month_df("specified_language","Arabic").collect()]
es = [r.countResult for r in count_by_month_df("specified_language","Spanish").collect()]
en = [r.countResult for r in count_by_month_df("specified_language","English").collect()]
cor = [r.countResult for r in count_by_month_df("specified_language","Corsican").collect()]
fr = [r.countResult for r in count_by_month_df("specified_language","French").collect()]
de = [r.countResult for r in count_by_month_df("specified_language","German").collect()]
tu = [r.countResult for r in count_by_month_df("specified_language","Turkish").collect()]
ru = [r.countResult for r in count_by_month_df("specified_language","Russian").collect()]
un = [r.countResult for r in count_by_month_df("specified_language","un").collect()]

'''
ar = count_by_month_df("specified_language","")
es = count_by_month_df("specified_language","")
en = count_by_month_df("specified_language","")
cor = count_by_month_df("specified_language","")
fr = count_by_month_df("specified_language","")
de = count_by_month_df("specified_language","")
tu = count_by_month_df("specified_language","")
ru = count_by_month_df("specified_language","")'''

fig, axes = plt.subplots()
plt.plot(it,label="it")
plt.plot(ar,label="ar")
plt.plot(es,label="es")
plt.plot(en,label="en")
plt.plot(cor,label="cor")
plt.plot(fr,label="fr")
plt.plot(de,label="de")
plt.plot(tu,label="tu")
plt.plot(ru,label="ru")
plt.plot(un,label="un")
plt.legend(loc="")
display(fig)


# COMMAND ----------

#molti un ... dobbiamo ispezionarli per vedere perché il classificatore fallisce
inspect_un = spark.sql("select text from tweets where language = 'un'")
display(inspect_un)
#come si può vedere sono post principalmente fatti di hashtag o url o riferimenti ... per questo visto che non c'è quasi testo per fare una classificazione (noi classifichiamo su clean text) ovviamente il classificatore di linguaggi si rifiuta ... però possiamo migliorare andando a visitare la pagina del messaggio classificato come un e recuperare la lingua del profilo utente


# COMMAND ----------

#focalizziamoci su italiani e inglesi
it_tweets = spark.sql("select * from tweets where language = 'Italian'")
en_tweets = spark.sql("select * from tweets where language = 'English'")
it_tweets.createOrReplaceTempView("it_tweets")
en_tweets.createOrReplaceTempView("en_tweets")

# COMMAND ----------

display(spark.sql("select hashtags from tweets"))

# COMMAND ----------

#prepara lista hashtags
import pyspark.sql.functions as f
hashtags_list_df = spark.sql("select hashtags from tweets").withColumn("hashtags", f.explode("hashtags"))
hashtags_list_df = hashtags_list_df.filter(f.length(col("hashtags"))>0)
hashtags_list_df.createOrReplaceTempView("hashtags_list")
display(hashtags_list_df)

it_hashtags_list_df = spark.sql("select hashtags from it_tweets").withColumn("hashtags", f.explode("hashtags"))#filter(f.size(f.col("hashtags"))>0)
it_hashtags_list_df = it_hashtags_list_df.filter(f.length(col("hashtags"))> 0)
it_hashtags_list_df.createOrReplaceTempView("it_hashtags_list")
display(it_hashtags_list_df)

en_hashtags_list_df = spark.sql("select hashtags from en_tweets").withColumn("hashtags", f.explode("hashtags"))#filter(f.size(f.col("hashtags"))>0)
en_hashtags_list_df = en_hashtags_list_df.filter(f.length(col("hashtags"))> 0)
en_hashtags_list_df.createOrReplaceTempView("en_hashtags_list")
display(en_hashtags_list_df)
#"select hashtags from en_tweets"

# COMMAND ----------

count_hashtags_rk = spark.sql("select hashtags, count(*) as countResult from hashtags_list group by hashtags order by countResult desc")
display(count_hashtags_rk)

# COMMAND ----------

#Group by value e poi fai la conta e fai ranking
#NON E' NECESSARIO FARE UN ISTOGRAMMA ... è molto poco rappresentativo
count_it_hashtags = spark.sql("select hashtags, count(*) as countResult from it_hashtags_list group by hashtags order by countResult desc")
display(count_it_hashtags)

# COMMAND ----------

count_en_hashtags = spark.sql("select hashtags, count(*) as countResult from en_hashtags_list group by hashtags order by countResult desc")
display(count_en_hashtags)

# COMMAND ----------

def post_hashtag_percentages(language):
  if language == "it":
    firstItem = spark.sql("select hashtags from it_tweets")
  elif language == "en":
    firstItem = spark.sql("select hashtags from en_tweets")
  elif language == "all":
    firstItem = spark.sql("select hashtags from tweets")

  firstItem = firstItem.withColumn("firstHashtag",firstItem.hashtags.getItem(0))
  count_without_hashtag = firstItem.filter("length(firstHashtag) = 0").count()
  count_with_hashtag = firstItem.filter("length(firstHashtag) > 0").count()
  count_with_hashtag1 = firstItem.filter("size(hashtags) = 1 and length(firstHashtag) > 0").count()
  count_with_hashtag2 = firstItem.filter("size(hashtags) = 2").count()
  count_with_hashtag3 = firstItem.filter("size(hashtags) = 3").count()
  count_with_hashtag4 = firstItem.filter("size(hashtags) = 4").count()
  count_with_hashtag5 = firstItem.filter("size(hashtags) = 5").count()
  count_with_hashtag_gt5 = firstItem.filter("size(hashtags) > 5").count()

  total = firstItem.count()
  print("------------- lan: "+language+" ---------------")
  print("Without hashtag: "+str(count_without_hashtag))
  print("With hashtag: "+str(count_with_hashtag))
  print(total)
  print("Percentage without hashtags: "+str(float(count_without_hashtag)/float(total)))
  print("Percentage with hashtags: "+str(float(count_with_hashtag)/float(total)))
  print("QUINDI NON POSSIAMO FARE TOPIC DETECTION BASANDOCI SOLO SUGLI HASHTAG ... visto che quasi il 45% dei dati non li ha")
  print("Relative percentage with 1: " + str(float(count_with_hashtag1)/float(count_with_hashtag)) + ", abs: "+str(count_with_hashtag1))
  print("Relative percentage with 2: " + str(float(count_with_hashtag2)/float(count_with_hashtag))+ ", abs: "+str(count_with_hashtag2))
  print("Relative percentage with 3: " + str(float(count_with_hashtag3)/float(count_with_hashtag))+ ", abs: "+str(count_with_hashtag3))
  print("Relative percentage with 4: " + str(float(count_with_hashtag4)/float(count_with_hashtag))+ ", abs: "+str(count_with_hashtag4))
  print("Relative percentage with 5: " + str(float(count_with_hashtag5)/float(count_with_hashtag))+ ", abs: "+str(count_with_hashtag5))
  print("Relative percentage with > 5: " + str(float(count_with_hashtag_gt5)/float(count_with_hashtag))+ ", abs: "+str(count_with_hashtag_gt5))
  print(count_with_hashtag1+count_with_hashtag2+count_with_hashtag3+count_with_hashtag4+count_with_hashtag5+count_with_hashtag_gt5)
  return {"count_without_hashtag":count_without_hashtag,"count_with_hashtag":count_with_hashtag, "count_with_hashtag1":count_with_hashtag1,"count_with_hashtag2":count_with_hashtag2,"count_with_hashtag3":count_with_hashtag3,"count_with_hashtag4":count_with_hashtag4,"count_with_hashtag5":count_with_hashtag5,"count_with_hashtag_gt5":count_with_hashtag1}

# COMMAND ----------

all_hashtag_d= post_hashtag_percentages("all")
it_hashtag_d = post_hashtag_percentages("it")
en_hashtag_d =post_hashtag_percentages("en")


#display(count_without_hashtag)

# COMMAND ----------

overall_hashtags_count = hashtags_list_df.count()
it_hashtags_count = it_hashtags_list_df.count()
en_hashtags_count = en_hashtags_list_df.count()
it_distinct_h_c = it_hashtags_list_df.distinct().count()
en_distinct_h_c = en_hashtags_list_df.distinct().count()
overall_distinct_h_c = hashtags_list_df.distinct().count()

num_posts = spark.sql("select * from tweets").count()
num_it_posts = spark.sql("select * from tweets where language = 'Italian'").count()
num_en_posts = spark.sql("select * from tweets where language = 'English'").count()

print("overall hashtags: "+str(overall_hashtags_count)+", avg hashtags per post: "+str(float(overall_hashtags_count)/float(num_posts)))
print("keep in mind that the language classification has some noise!")
print("italian hashtags: " +str(it_hashtags_count) + " , percentage of total: "+str(float(it_hashtags_count)/float(overall_hashtags_count)) + ", avg hashtags per post: "+str(float(it_hashtags_count)/float(num_it_posts)))
print("english hashtags: "+ str(en_hashtags_count)+ " , percentage of total: "+str(float(en_hashtags_count)/float(overall_hashtags_count)) + ", avg hashtags per post: "+str(float(en_hashtags_count)/float(num_en_posts)))

print("overall distinct hashtags: "+str(overall_distinct_h_c)+ ", avg replication of same hashtag: "+str(float(overall_hashtags_count)/float(overall_distinct_h_c)))
print("italian distinct hashtags: "+ str(it_distinct_h_c) + ", percentage of total: " + str(float(it_distinct_h_c)/float(overall_distinct_h_c)) + ", avg replication of same hashtag: "+str(float(it_hashtags_count)/float(it_distinct_h_c)))
print("english distinct hashtags: "+ str(en_distinct_h_c) +  ", percentage of total: " + str(float(en_distinct_h_c)/float(overall_distinct_h_c))+ ", avg replication of same hashtag: "+str(float(en_hashtags_count)/float(en_distinct_h_c)))


# COMMAND ----------

def plotPercentagesOfHashtags(d):
  fig, ax = plt.subplots(2)
  first_pie_data = [d["count_without_hashtag"],d["count_with_hashtag"]]
  first_pie_labels = ["without","with"]
  second_pie_data = [d["count_with_hashtag1"],d["count_with_hashtag2"],d["count_with_hashtag3"],d["count_with_hashtag4"],d["count_with_hashtag5"],d["count_with_hashtag_gt5"]]
  second_pie_labels = ["1","2","3","4","5",">5"]
  ax[0].pie(first_pie_data,labels = first_pie_labels,autopct='%1.1f%%',
          shadow=True, startangle=90,pctdistance = 2.)
  ax[1].pie(second_pie_data,labels = second_pie_labels,autopct='%1.1f%%',
          shadow=True, startangle=90,pctdistance = 1.4)
  ax[0].axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
  ax[1].axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.

  display(fig)

# COMMAND ----------

plotPercentagesOfHashtags(all_hashtag_d)

# COMMAND ----------

plotPercentagesOfHashtags(it_hashtag_d)

# COMMAND ----------

plotPercentagesOfHashtags(en_hashtag_d)

# COMMAND ----------

from pyspark.sql.types import ArrayType,StringType
def remove_duplicates_in_array(a):
  temp=[]
  for str in a:
    temp.append(str.lower())
  return list(set(temp))

my_udf = udf(remove_duplicates_in_array, ArrayType(StringType()))
for_hashtag_association = clean_df.withColumn("hashtags", my_udf("hashtags"))
display(for_hashtag_association)

# COMMAND ----------

from pyspark.ml.fpm import FPGrowth

#minSupport = frequenza minima con cui si deve presentare per essere considerato frequente ... considerando che abbiamo un dataset di 10.000 e passa, chiedere 1/1000 significa chiedere si verifichi almeno 10 volte

fpGrowth = FPGrowth(itemsCol="hashtags", minSupport=0.002, minConfidence=0.3)
model = fpGrowth.fit(for_hashtag_association)


# COMMAND ----------

# Display frequent itemsets.
freqItemsets = model.freqItemsets.orderBy("freq",ascending = False)
display(freqItemsets)
#i risultati più interessanti non sono i più frequenti nel nostro caso ...
oneItem = freqItemsets.filter(size(col("items"))==1)
twoItem = freqItemsets.filter(size(col("items"))==2)
threeItem = freqItemsets.filter(size(col("items"))==3)


# COMMAND ----------

print("ALL")
freqItemsets.agg({"freq":"avg"}).show()
freqItemsets.agg({"freq":"std"}).show()
print("ONE")
oneItem.agg({"freq":"avg"}).show()
oneItem.agg({"freq":"std"}).show()
print("TWO")
twoItem.agg({"freq":"avg"}).show()
twoItem.agg({"freq":"std"}).show()
print("THREE")
threeItem.agg({"freq":"avg"}).show()
threeItem.agg({"freq":"std"}).show()

# COMMAND ----------

oneItem = oneItem.filter("freq < 200 and freq > 30")
display(oneItem)

# COMMAND ----------

'''def unsortItemset(t):
  return list(set(t))

unsort_udf = udf(unsortItemset, ArrayType(StringType()))'''

twoItem = twoItem.filter("freq < 200 and freq > 30")
display(twoItem)


# COMMAND ----------

#threeItem.createOrReplaceTempView("3Itemset")
#threeItem = spark.sql("select items, sum(freq) as freq from 3Itemset group by items order by freq desc")#unsort_udf("items").alias("items"),sum(col("freq"))).groupBy("items")
#twoItem = twoItem.filter("freq < 200 and freq > 30")
#threeItem.count() #42
display(threeItem)

# COMMAND ----------

from pyspark.ml.feature import RegexTokenizer, Tokenizer
from pyspark.ml.feature import CountVectorizer , IDF, StopWordsRemover
from pyspark.ml.linalg import Vector, Vectors
from pyspark.ml.clustering import LDA, LDAModel
from pyspark.ml.feature import NGram,HashingTF
from pyspark.ml import Pipeline
#fai clustering
#non ci interessa il remover in sé (tanto non conterrà hashtag)
stopwords = StopWordsRemover().loadDefaultStopWords("english")
stopwords.extend([u"#lake",u"#como",u"#italy", u"#co",u"#lakecomo",u"#lagocomo",u"#comolake",u"#lagodicomo",u"#italia",u"#italien",u"#lago"])
remover = (StopWordsRemover()
                .setInputCol("items")
                .setOutputCol("clean_items")
                .setStopWords(stopwords))
#int_freqItems = remover.transform(freqItemsets)

counts = CountVectorizer(inputCol = "clean_items", outputCol="raw_features", vocabSize = 10000, minDF = 2.0)

idf = IDF(inputCol="raw_features",outputCol="features")

num_topics = 6
max_iterations = 50

lda = LDA(k=num_topics, seed=1, optimizer="em")

pipeline = Pipeline().setStages([remover,counts,idf,lda]) 
pipeline_model = pipeline.fit(freqItemsets)


# COMMAND ----------

lda_model = pipeline_model.stages[3]
vocabArray = pipeline_model.stages[1].vocabulary

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
    
#di base ci sono 4 topics (ho usato 6 topics, ma ne escono 2 "duplicati"):
#NON ci interessa avere un clustering perfetto, vogliamo giusto sapere quali sono gli argomenti principali
# - topic di turisti generici (lago, italia, travel, duomo, sky)
# - tatuaggi, gastronomia e mojiterie
# - lago, villadeste e lovecars
# - animali e gattini

# COMMAND ----------


# Display generated association rules.
display(model.associationRules.orderBy("confidence",ascending = False))


# COMMAND ----------

from pyspark.mllib.linalg import Vectors, VectorUDT
from pyspark.sql.types import FloatType
import operator

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

def extractTopics(transformed):
  transformed.createOrReplaceTempView("transformed")
  estrai_topic = sqlContext.sql("select *, get_top_k_topics_idx(topicDistribution) as topTopics,get_top_1_topics_idx(topicDistribution) as firstTopic  from transformed")
  #, get_top_k_topics_1(topicDistribution) as topTopics
  display(estrai_topic)
  estrai_topic.createOrReplaceTempView("extracted_transformed")
  return estrai_topic

# COMMAND ----------

transformed = pipeline_model.transform(freqItemsets)
new_transformed = extractTopics(transformed)
dislpay(new_transformed)

# COMMAND ----------

#PCA
#poco significativa la visualizzazione 
from pyspark.ml.feature import PCA 
from pyspark.ml.feature import StandardScaler
#normalizzazione prima della PCA
counts = CountVectorizer(inputCol = "items", outputCol="raw_features2", vocabSize = 10000, minDF = 2.0)
counter = counts.fit(new_transformed)
counted = counter.transform(new_transformed)
scaler = StandardScaler(inputCol="raw_features2", outputCol="scaledFeatures",withStd=True, withMean=False)
scalerModel = scaler.fit(counted)
scaledData = scalerModel.transform(counted)

pca = PCA(k=2, inputCol = "raw_features2", outputCol = "pca")
model = pca.fit(scaledData)
transformed_df = model.transform(scaledData)
display(transformed_df)

# COMMAND ----------

#la PCA è inefficace per la riduzione della dimensionalità!
#quindi se il clustering non viene visualizzato bene, è normale
#d'altra parte non possiamo giocare su altro per migliorare la PCA
#il punto è che abbiamo dati ad alta dimensionalità, con vettori molto sparsi e non possiamo scegliere le componenti 
model.explainedVariance

# COMMAND ----------

import matplotlib.pyplot as plt
colors = [ (1.0, 0.0, 0.0), (0.0,1.0,0.0),  (0.0, 0.2,0.0),
             (1.0, 1.0, 0.0),  (1.0, 0.0, 1.0),  (0.0, 1.0, 1.0),  (0.0, 0.0, 0.0), 
             (0.5, 0.5, 0.5),  (0.8, 0.8, 0.0), (0.3, 0.3, 0.3)]
def plotClusters(first_principal_component, second_principal_component,topics):
  fig, axes = plt.subplots()
  for t in range(0,len(first_principal_component)):
    axes.plot(first_principal_component[t],second_principal_component[t],"o",color = colors[topics[t]])
  display(fig)

# COMMAND ----------

#la visualizzazione è poco significativa visto anche l'elevato numero di items vuoti (a causa di stopwords removal + ragioni viste prima
first_principal_component = [row.pca[0] for row in transformed_df.collect()]
second_principal_component = [row.pca[1] for row in transformed_df.collect()]
topics = [int(row.firstTopic) for row in transformed_df.collect()]
plotClusters(first_principal_component, second_principal_component,topics)

# COMMAND ----------

hist_df = spark.sql('''
  SELECT timestamp, COUNT(id) as countResult
  FROM tweets
  GROUP BY timestamp
  ORDER BY timestamp 
''') 
########################################
#########################################
##########################################
from pyspark.sql.types import FloatType
from pyspark.sql.functions import lit

hist_df = hist_df.filter(col('timestamp').isNotNull())
#cast per dopo, altrimenti era long e creava problemi
hist_df = hist_df.withColumn("countResult", hist_df["countResult"].cast(FloatType()))
hist_df = hist_df.withColumn("language",lit("all"))
display(hist_df)


# COMMAND ----------

hist_df.describe(['countResult']).show()


# COMMAND ----------

#poiché la media è 28.3 e la deviazione standard è 11.6, allora la "regoletta" degli outlier è 28.3 + 2*11.6 = 51
outliers = hist_df.filter("countResult > 51")
display(outliers)

# COMMAND ----------

from polyglot.downloader import downloader
downloader.download("embeddings2.en")
downloader.download("ner2.en")
downloader.download("embeddings2.it")
downloader.download("ner2.it")

# COMMAND ----------

from polyglot.text import Text
def extract_NN_by_lan(sent,lan):
  entities = []
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


# COMMAND ----------

hist_df_it_en = spark.sql('''
  SELECT timestamp, COUNT(id) as countResult,language
  FROM tweets
  WHERE language = 'Italian' or language = 'English'
  GROUP BY timestamp, language
  ORDER BY timestamp 
''') 
from pyspark.sql.types import FloatType
hist_df_it_en = hist_df_it_en.filter(col('timestamp').isNotNull())
#cast per dopo, altrimenti era long e creava problemi
hist_df_it_en = hist_df_it_en.withColumn("countResult", hist_df_it_en["countResult"].cast(FloatType()))
display(hist_df_it_en.union(hist_df))


# COMMAND ----------

print("Italian")
hist_df_it_en.filter("language = 'Italian'").describe(['countResult']).show()
print(4.95 + 2*3)
print("English")
hist_df_it_en.filter("language = 'English'").describe(['countResult']).show()
print(13.7+2*7)

# COMMAND ----------

from pyspark.ml.stat import ChiSquareTest
import pandas as pd

temp_chi_it = hist_df_it_en.select(col("timestamp"),col("countResult").alias("c1") ).filter("language = 'Italian'")
temp_chi_en = hist_df_it_en.select(col("timestamp"),col("countResult").alias("c2") ).filter("language = 'English'")

pd_chi_it = temp_chi_it.toPandas()
pd_chi_it["timestamp"] = pd.to_datetime(pd_chi_it["timestamp"], format = "%Y-%m-%d")
pd_chi_it = pd_chi_it.set_index('timestamp')
pd_chi_it_s= pd_chi_it["c1"]

pd_chi_en = temp_chi_en.toPandas()
pd_chi_en["timestamp"] = pd.to_datetime(pd_chi_en["timestamp"], format = "%Y-%m-%d")
pd_chi_en = pd_chi_en.set_index('timestamp')
pd_chi_en_s= pd_chi_en["c2"]

pd_chi_all = hist_df.toPandas()
pd_chi_all["timestamp"] = pd.to_datetime(pd_chi_all["timestamp"], format = "%Y-%m-%d")
pd_chi_all = pd_chi_all.set_index('timestamp')
pd_chi_all_s= pd_chi_all["countResult"]

print("################################")
print("################################")
print("################################")
print("correlazione italiano-inglese")
print(pd_chi_it_s.corr(pd_chi_en_s))
print("quindi la correlazione tra i dati è molto bassa")
print("correlazione italiano-tutti")
print(pd_chi_it_s.corr(pd_chi_all_s))
print("quindi la correlazione è trascurabile")
print("correlazione inglese-tutti")
print(pd_chi_all_s.corr(pd_chi_en_s))
print("quindi la correlazione è molto alta")
print("################################")
print("################################")
print("################################")


# COMMAND ----------

display(spark.sql('''
  SELECT *
  FROM tweets
  WHERE language = 'English'
  ORDER BY timestamp 
'''))

# COMMAND ----------

display(clean_df.select(col("clean_text\r")))

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType

extract_entities_en = udf(lambda text: extract_NN_by_lan(text,"en") , ArrayType(StringType()))
extract_entities_it = udf(lambda text: extract_NN_by_lan(text,"it") , ArrayType(StringType()))

en_tw = spark.sql('''
  SELECT *
  FROM tweets
  WHERE language = 'English'
  ORDER BY timestamp 
''')

en_tw = en_tw.select("timestamp","clean_text\r")

it_tw = spark.sql('''
  SELECT *
  FROM tweets
  WHERE language = 'Italian'
  ORDER BY timestamp 
''')

it_tw = it_tw.select("timestamp","clean_text\r")

en_tw_entities = en_tw.withColumn('entities',extract_entities_en(en_tw["clean_text\r"]))
it_tw_entities = it_tw.withColumn("entities",extract_entities_it(it_tw["clean_text\r"]))

# COMMAND ----------

display(it_tw_entities)

# COMMAND ----------

from pyspark.sql.functions import count,sum

def getEntitiesInMultipleDataframes(date,data1,data2):
  temp1 = getEntitiesInTweets(date,data1)
  temp2 = getEntitiesInTweets(date,data2)
  result = temp1.union(temp2)
  #result = result.groupBy("entities").agg(sum("count").alias("tot")).orderBy("tot",ascending = False)
  #display(result)
  result.show(result.count(),False)
  return result

def getEntitiesInTweets(date, data):
  temp = data.filter("timestamp = cast('"+date+"' as date)")
  temp = temp.select("entities")
  temp = temp.withColumn("entities",f.explode("entities"))
  temp = temp.filter(f.length(col("entities")) >0)
  temp = temp.groupBy("entities").agg(count("entities").alias("count")).orderBy("count", ascending = False)
  #display(temp)
  return temp

def getHashtagsInTweets(date,data):
  temp = data.filter("timestamp = cast('"+date+"' as date)")
  temp = temp.select("hashtags")
  temp = temp.withColumn("hashtags",f.explode("hashtags"))
  temp = temp.filter(f.length(col("hashtags")) >0)
  temp = temp.groupBy("hashtags").agg(count("hashtags").alias("count")).orderBy("count", ascending = False)
  #display(temp)
  temp.show(temp.count(),False)
  return temp


# COMMAND ----------

getEntitiesInMultipleDataframes("2016-05-22",en_tw_entities,it_tw_entities)
getHashtagsInTweets("2016-05-22",clean_df)

# COMMAND ----------

getEntitiesInMultipleDataframes("2016-03-19",en_tw_entities,it_tw_entities)
getHashtagsInTweets("2016-03-19",clean_df)

# COMMAND ----------

getEntitiesInMultipleDataframes("2016-03-28",en_tw_entities,it_tw_entities)
getHashtagsInTweets("2016-03-28",clean_df)

# COMMAND ----------

getEntitiesInMultipleDataframes("2016-04-24",en_tw_entities,it_tw_entities)
getHashtagsInTweets("2016-04-24",clean_df)

# COMMAND ----------

getEntitiesInMultipleDataframes("2016-06-18",en_tw_entities,it_tw_entities)
getHashtagsInTweets("2016-06-18",clean_df)

# COMMAND ----------

getEntitiesInMultipleDataframes("2016-06-23",en_tw_entities,it_tw_entities)
getHashtagsInTweets("2016-06-23",clean_df)

# COMMAND ----------

getEntitiesInMultipleDataframes("2016-06-24",en_tw_entities,it_tw_entities)
getHashtagsInTweets("2016-06-24",clean_df)

# COMMAND ----------

getEntitiesInMultipleDataframes("2016-06-26",en_tw_entities,it_tw_entities)
getHashtagsInTweets("2016-06-26",clean_df)

# COMMAND ----------

getEntitiesInMultipleDataframes("2016-07-27",en_tw_entities,it_tw_entities)
getHashtagsInTweets("2016-07-27",clean_df)

# COMMAND ----------

getEntitiesInMultipleDataframes("2016-08-02",en_tw_entities,it_tw_entities)
getHashtagsInTweets("2016-08-02",clean_df)

# COMMAND ----------

getEntitiesInMultipleDataframes("2016-08-07",en_tw_entities,it_tw_entities)
getHashtagsInTweets("2016-08-07",clean_df)

# COMMAND ----------

getEntitiesInMultipleDataframes("2016-08-08",en_tw_entities,it_tw_entities)
getHashtagsInTweets("2016-08-08",clean_df)

# COMMAND ----------

getEntitiesInMultipleDataframes("2016-08-13",en_tw_entities,it_tw_entities)
getHashtagsInTweets("2016-08-13",clean_df)

# COMMAND ----------

getEntitiesInMultipleDataframes("2016-09-15",en_tw_entities,it_tw_entities)
getHashtagsInTweets("2016-09-15",clean_df)

# COMMAND ----------

summation = spark.sql("select month(timestamp) as month,sum(likes),sum(num_replies),sum(reshares) from tweets group by month order by month")
display(summation)
#quindi a dicembre e maggio ci sono parecchi like, e i reshare sono molto accentuati ... le risposte sono più o meno costanti, quasi sempre insignificanti
#come ci spieghiamo questi comportamenti? Lo vedremo dopo ... ma sono principalmente dovuti a "fattori esterni" (i.e. gente con tanti followers che fa messaggi mentre è a como)

# COMMAND ----------

mean = spark.sql("select month(timestamp) as month,avg(likes),avg(num_replies),avg(reshares) from tweets group by month order by month")
display(mean)


# COMMAND ----------

stdev = spark.sql("select month(timestamp) as month,std(likes) as likes,std(num_replies) as replies,std(reshares) as reshares from tweets group by month order by month")
display(stdev)
#dicembre e maggio hanno grandi fluttuazioni, soprattutto per reshare e replies, quindi vale la pena dare un'occhiata

# COMMAND ----------

maxdf = spark.sql("select month(timestamp) as month,max(likes) as xlikes,max(num_replies) as xreplies,max(reshares) as xreshares from tweets group by month order by month")
display(maxdf)
#maggio, giugno e dicembre hanno post con alto numero dilike (quello di dicembre addirittura vale un terzo di tutti i like di quel mese!)

# COMMAND ----------

maximum_likes_post = spark.sql("select * from tweets T where (T.likes,month(T.timestamp)) in (select max(T1.likes),month(T1.timestamp) as month from tweets T1 group by month)")
display(maximum_likes_post)
#infatti i più grandi likes provengono da MatteoPelusi (due personaggi web moltofamosi), relativi ad un evento (che però è a modena)
#inoltre tutti i post con tanti like sono principalmente legati ad influencers/sportivi/artisti/personaggi famosi con parecchi followers 
#il testo dei loro tweet e per lo più poco significativo e dice poco su como, la grande reattività è legata a loro <- Queste anomalie sono poco rappresentative

# COMMAND ----------

 mindf = spark.sql("select month(timestamp) as month, min(likes) as nlikes, min(num_replies) as nnum_replies, min(reshares) as nreshares from tweets group by month order by month")
display(mindf)
#tutto a zero, ma questa è una cosa banale

# COMMAND ----------

summation = spark.sql("select month(timestamp) as month,sum(likes),sum(num_replies),sum(reshares) from tweets group by month order by month")
display(summation.drop("month"))
#sum(likes) e sum(reshares) potrebbero essere correlati 
#le restanti coppie di variabili non sembrano significativamente correlate

# COMMAND ----------

res_rep_corr = summation.stat.corr("sum(reshares)","sum(likes)")
print("Correlation reshares-likes: "+str(res_rep_corr))
res_rep_corr = summation.stat.corr("sum(reshares)","sum(num_replies)")
print("Correlation reshares-num_replies: "+str(res_rep_corr))
res_rep_corr = summation.stat.corr("sum(likes)","sum(num_replies)")
print("Correlation likes-num_replies: "+str(res_rep_corr))
#ovviamente su un campione così piccolo questi dati sono poco significativi! però si accordano relativamente bene alla statistica descrittiva su fatta

# COMMAND ----------

summation = spark.sql("select month(timestamp) as month,sum(likes),sum(num_replies),sum(reshares) from tweets group by month order by month")
display(summation)


# COMMAND ----------

display(summation)


# COMMAND ----------

display(summation)


# COMMAND ----------

import pandas as pd
#ora invece ragioniamo considerando ogni singolo post come punto a se stante
summation = spark.sql("select likes,num_replies,reshares from tweets")
summation_pd = summation.select("likes").toPandas()
fig, ax = plt.subplots(1,2)
ax[0].set_title('likes, with outliers')
ax[0].boxplot(summation_pd["likes"].values, notch=True)
ax[1].set_title('likes, without outliers')
ax[1].boxplot(summation_pd["likes"].values, showfliers=False)
display(fig)

# COMMAND ----------

summation_pd = summation.select("num_replies").toPandas()
fig, ax = plt.subplots(1,2)
ax[0].set_title('num_replies, with outliers')
ax[0].boxplot(summation_pd["num_replies"].values, notch=True)
ax[1].set_title('num_replies, without outliers')
ax[1].boxplot(summation_pd["num_replies"].values, showfliers=False)
display(fig)

# COMMAND ----------

summation_pd = summation.select("reshares").toPandas()
fig, ax = plt.subplots(1,2)
ax[0].set_title('reshares, with outliers')
ax[0].boxplot(summation_pd["reshares"].values, notch=True)
ax[1].set_title('reshares, without outliers')
ax[1].boxplot(summation_pd["reshares"].values, showfliers=False)
display(fig)

# COMMAND ----------

user_tweet_counters = spark.sql("select creator, count(*) as total from tweets group by creator order by total desc")
print(user_tweet_counters.count())
#abbiamo quindi 3114 utenti distinti ... molto poco

# COMMAND ----------

display(user_tweet_counters)#granularità fine

# COMMAND ----------

#mostra l'utente più attivo e i suoi post (dopo)
display(spark.sql("select distinct creator from tweets where creator in (select creator from tweets T group by creator having count(*) > 420)"))

# COMMAND ----------

display(spark.sql("select * from tweets where creator = 'wizardxp'"))

# COMMAND ----------

#ora vediamo gli altri più frequenti
display(spark.sql("select distinct creator from tweets where creator in (select creator from tweets T group by creator having count(*) >= 150 and count(*) < 400)"))

# COMMAND ----------

display(spark.sql("select * from tweets where creator = 'Greta_Scacchi'"))

# COMMAND ----------

display(spark.sql("select * from tweets where creator = 'Seventhheavenm'"))
#il nostro amico arabo DA SOLO spiega tutto il picco che c'è stato nel periodo di maggio-giugno ... praticamente ha fatto più di 150 post solo lui ...
#quindi non ha a che fare col turismo

# COMMAND ----------

display(spark.sql("select * from tweets where creator = 'MinervaAlex'"))
#questo invece è un promoter di Sciarada (una mojiteria)


# COMMAND ----------

#scendiamo ancora un po' 
display(spark.sql("select distinct creator from tweets where creator in (select creator from tweets T group by creator having count(*) > 60 and count(*) < 150)"))

# COMMAND ----------

display(spark.sql("select * from tweets where creator = 'vittoriatattoo'"))
#questo è un promoter di un tatuatore (che si trova nella zona via alessandro volta)

# COMMAND ----------

display(spark.sql("select * from tweets where creator = 'richardwharram'"))
#semplicemente un inglese (che ritorna spesso a como) che si spassa a twittare come un forsennato (non avrà niente da fare ...)

# COMMAND ----------

display(spark.sql("select * from tweets where creator = 'diamondsport'"))
#un manager sportivo molto attivo su twitter (non si promuove particolarmente)

# COMMAND ----------

display(spark.sql("select * from tweets where creator = 'GastroCasti'"))
#gastronomia castiglioni che si promuove

# COMMAND ----------

display(spark.sql("select * from tweets where creator = 'ManjaresMylen'"))
#sono solo una coppia asiatica abbastanza iperattiva su twitter, ma niente di significativo

# COMMAND ----------

display(spark.sql("select * from tweets where creator = 'Sevi_school'"))
#language coach, pronunciation specialist, Author&Public Speaker 

# COMMAND ----------

display(spark.sql("select * from tweets where creator = 'Casariaquila'"))
#catechista che pubblica roba molto frequentemente (quasi sempre su dio)

# COMMAND ----------

display(spark.sql("select * from tweets where creator = 'ComuneComo'"))
#credo sia banale ...

# COMMAND ----------

#da qui in giù sono tra 60 e 80
display(spark.sql("select * from tweets where creator = 'kartenquizde'"))
#un quiz di geografia

# COMMAND ----------

display(spark.sql("select * from tweets where creator = 'criblueyes'"))
#una persona comune che però condivide ogni benedetto pasto/drink/aperitivo

# COMMAND ----------

display(spark.sql("select * from tweets where creator = 'LiS2Shoot'"))
#persona che usa swarmapp (un'app che condivide tweet di localizzazione e crea dei log per ricordarsi le proprie esperienze ... alzheimer)

# COMMAND ----------

display(spark.sql("select * from tweets where creator = 'marika1987'"))
#anche lei swarmapp

# COMMAND ----------

#scendiamo ancora più giù
display(spark.sql("select distinct creator from tweets where creator in (select creator from tweets T group by creator having count(*) >= 47 and count(*) < 60)"))

# COMMAND ----------

display(spark.sql("select * from tweets where creator = 'latombolillo'"))
#iperattiva, molto legata al teatro sociale e alla vita mondana, ma di base una persona normale

# COMMAND ----------

display(spark.sql("select * from tweets where creator = 'cillip'"))

# COMMAND ----------

display(spark.sql("select * from tweets where creator = 'Fcarphoto'"))
#promozione del concorso d'eleganza a villa d'este
#Automotive photographer. Shooting for Cars&Coffee, Lovecars, GTspirit

# COMMAND ----------

display(spark.sql("select * from tweets where creator = 'AllExclusiveUS'"))
#agenzia pubblicitaria che promuove negozi da sposi e ristoranti

# COMMAND ----------

display(spark.sql("select * from tweets where creator = 'LaSaraVa'"))
#semplicemente persona che fa "turismo" (anche se è di como) e gira per como

# COMMAND ----------



# COMMAND ----------

display(user_tweet_counters) #ho usato questo scatterplot per selezionare quanti tweet andarmi a vedere manualmente

# COMMAND ----------

#conclusione ... twitter non è proprio il miglior posto per sapere di cosa parla la gente a como, eh ... è pieno di rumore (o c'è roba banale (lago) o ci sono post molto ripetitivi di attività o di poche persone ... i pattern interessanti sono nel medio)
