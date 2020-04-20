# Databricks notebook source
#da rimuovere ... non serve a niente 
tweet = spark.read.format("csv").option("header", "true").option("multiline",'true').option("inferSchema", 'true').load("dbfs:/FileStore/tables/AllQuery3Tweets2017.csv")
print(tweet.count())
tweet = tweet.dropna()
tweet = tweet.na.fill("")
#get Date instead of String for Timestamp
from pyspark.sql.functions import *
tweet_with_date = tweet.withColumn("timestamp",from_unixtime(unix_timestamp('timestamp', 'HH:mm - dd MM yyyy')).cast("date")) #puoi anche usare "timestamp" per conservare anche l'ora, ma per ora non ti serve
tweet_with_date.createOrReplaceTempView("tweet_with_date")
tweet = spark.sql("select timestamp, count(*) as total from tweet_with_date group by timestamp order by timestamp asc")
display(tweet)

# COMMAND ----------

from pyspark.sql.types import LongType, StringType, StructField, StructType, BooleanType, ArrayType, IntegerType, DateType
from pyspark.sql.functions import substring
#load data
'''customSchema = StructType([
        StructField("name", StringType(), True),
        StructField("freePlaces", IntegerType(), True),
        StructField("weekday", StringType(), True),
        StructField("month", IntegerType(), True),
        StructField("day", IntegerType(), True),
        StructField("year", IntegerType(), True),
        StructField("hour", StringType(), True)])'''
park_data = spark.read.format("csv").option("inferSchema", 'true').load("dbfs:/FileStore/tables/AllFiltParkPred.txt")
park_data = park_data.selectExpr("_c0 as name", "_c1 as free","_c2 as weekday","_c3 as month","_c4 as day","_c5 as year","_c6 as hour")
park_data = park_data.withColumn("hour",substring("hour",0,2))
park_data = park_data.withColumn("hour",park_data["hour"].cast(IntegerType()))
park_data.createOrReplaceTempView("dparking")
#display(park_data)
#park_data = park_data.withColumn("")

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import from_utc_timestamp

weather = spark.read.format("csv").option("header", "true").option("multiline",'true').option("inferSchema", 'true').load("dbfs:/FileStore/tables/weather.csv")
weather = weather.withColumn('date', weather.date.cast('timestamp'))
weather = weather.withColumn('date', from_utc_timestamp(weather.date, "CET"))
#weather = weather.withColumn("date",substring("date",0,2))
#weather = weather.withColumn("date", unix_timestamp(weather["date"], "yyyy-MM-dd hh"))
display(weather)
weather.createOrReplaceTempView("weather")
weather_pd = weather.toPandas()
weather_pd["date"] = pd.to_datetime(weather_pd["date"], format = "%Y-%m-%d-%H")
weather_pd = weather_pd.set_index('date')
#weather_pd["temperature"]

# COMMAND ----------

#FAI ESTREMA ATTENZIONE: ALCUNI DATI MANCANO DAL FILE METEREOLOGICO!!!! RAGIONE PER CUI DOVRAI FARE PARTICOLARE ATTENZIONE CHE QUANDO FARAI IL JOIN CON GLI ALTRI DATI, LADDOVE MANCHINO DATI METEREOLOGICI, GLI ALTRI DATI VENGANO SCARTATI ..
check_weather = spark.sql("select * from weather where month(date) = 08")
display(check_weather)#chiaramente mancano i dati dal 5 al 10 agosto, ma anche altrove

# COMMAND ----------

clean_park = spark.sql("select name, avg(free) as avgfree, weekday, month, day, hour from dparking where (month >=6 and month <= 10) and (year = 2016)  group by name, weekday, month, day, hour order by name, month,day,hour")
display(clean_park)

# COMMAND ----------

clean_park.createOrReplaceTempView("cparking")
display(spark.sql("select distinct(name) from cparking"))

# COMMAND ----------

#nome: codice, area
parks = {"Auguadri":("101",1),#Auguardi, area 1
         "Valduce":("104",1),#Valduce, area 1
         "Valmulini":("123",7),#Valmulini, area 7 (ma leggermente fuori mappa)
         "Castelnuovo":("107",7),#Castelnuovo, area 7
         "Centro Lago":("103",4),#Centro Lago, area 4
         "San Giovanni":("109",4),#San Giovanni, area 4
         "Sirtori":("105",1)     #Sirtori, confine area 1 e 7   
        }

park_data_dict = {}
park_data_with_date_dict = {}
park_data_visual_dict = {}
for name in parks:
  park_data_dict[parks[name][0]] = spark.sql("select * from cparking where name = "+parks[name][0])
  park_data_with_date_dict[parks[name][0]] = park_data_dict[parks[name][0]].select("name","avgfree","weekday", concat(lit("2016-"),col("month"),lit("-"),col("day"),lit("-"),col("hour")).alias("date"))
  park_data_visual_dict[parks[name][0]] = park_data_dict[parks[name][0]].select("name","avgfree","weekday", concat(lit("2016-"),col("month"),lit("-"),col("day")).alias("date").cast(DateType()))

# COMMAND ----------

display(park_data_visual_dict["101"])

# COMMAND ----------

#strano quel plateau a fine giugno, però sono tutti dati registrati con uguale numero di parecheggio ... guasto del sistema?
display(park_data_dict["101"].filter("day = 26 and month = 6"))

# COMMAND ----------

display(park_data_visual_dict["104"])


# COMMAND ----------

#lo stesso qui, immagino sia un guasto del sistema
display(park_data_visual_dict["123"])


# COMMAND ----------

display(park_data_visual_dict["107"])


# COMMAND ----------

#qui forse c'è stata una chiusura del parcheggio (con svuotamento) o un guasto del sistema sempre a fine giugno
display(park_data_visual_dict["103"])


# COMMAND ----------

display(park_data_visual_dict["109"])


# COMMAND ----------

display(park_data_visual_dict["105"])

# COMMAND ----------

import pandas as pd
def pandas_series(df):
  df_pandas = df.toPandas()
  df_pandas["date"] = pd.to_datetime(df_pandas["date"], format = "%Y-%m-%d-%H")
  df_pandas.dtypes
  df_pandas = df_pandas.set_index('date')
  return df_pandas 

import matplotlib.pyplot as plt
def visualize(df_pandas):
  fig, axes = plt.subplots()
  axes.plot(df_pandas["avgfree"])
  fig.autofmt_xdate()
  display(fig)

# COMMAND ----------

park_data_pandas_df = {}
for name in parks:
  park_data_pandas_df[parks[name][0]] = pandas_series(park_data_with_date_dict[parks[name][0]])

# COMMAND ----------

  visualize(park_data_pandas_df["101"])

# COMMAND ----------

  visualize(park_data_pandas_df["104"])

# COMMAND ----------

  visualize(park_data_pandas_df["123"])

# COMMAND ----------

  visualize(park_data_pandas_df["107"])

# COMMAND ----------

  visualize(park_data_pandas_df["103"])

# COMMAND ----------

  visualize(park_data_pandas_df["109"])

# COMMAND ----------

  visualize(park_data_pandas_df["105"])

# COMMAND ----------

telco_count_hour_cluster = spark.read.format("csv").option("header", "true").option("multiline",'true').option("inferSchema", 'true').load("dbfs:/FileStore/tables/count_hour_cluster.csv")
telco_count_hour_cluster.createOrReplaceTempView("telco_hour_cluster")

# COMMAND ----------

display(telco_count_hour_cluster)

# COMMAND ----------

from pyspark.sql.types import TimestampType
from pyspark.sql.functions import *

clusters = [1,2,3,4,5,6,7]
telco_data_dict = {}
telco_data_with_date_dict = {}
telco_data_visual_dict = {}
for name in clusters:
  telco_data_dict[name] = spark.sql("select * from telco_hour_cluster where cluster = "+str(name)+" order by date, hour")
  telco_data_with_date_dict[name] = telco_data_dict[name].select(concat(lit("2016-"),month(telco_data_dict[name].date),lit("-"),dayofmonth(telco_data_dict[name].date),lit("-"),col("hour")).alias("date"),"count" )
  telco_data_visual_dict[name] = telco_data_dict[name].select(concat(lit("2016-"),month(telco_data_dict[name].date),lit("-"),dayofmonth(telco_data_dict[name].date)).alias("date").cast(DateType()),"count")
  telco_data_visual_dict[name] = telco_data_visual_dict[name].groupBy("date").agg({"count":"avg"}).orderBy("date")

# COMMAND ----------

telco_data_dict[1]["count"]

# COMMAND ----------

fig, axes = plt.subplots(4,1)
counter = 0
for d in [1,2,3,4]:
  pd_s = pandas_series(telco_data_dict[d])
  #dates = [r.date for r in telco_data_dict[d].collect()]
  #counts = [r.count for r in telco_data_dict[d].collect()]
  axes[d-1].plot(pd_s["count"])
  fig.autofmt_xdate()
  counter = counter+1

display(fig)

# COMMAND ----------

fig, axes = plt.subplots(3,1)
counter = 0
for d in [5,6,7]:
  pd_s = pandas_series(telco_data_dict[d])
  #dates = [r.date for r in telco_data_dict[d].collect()]
  #counts = [r.count for r in telco_data_dict[d].collect()]
  axes[d-5].plot(pd_s["count"])
  fig.autofmt_xdate()
  counter = counter+1

display(fig)

# COMMAND ----------

display(telco_data_visual_dict[1])
 

# COMMAND ----------

display(telco_data_visual_dict[2])

# COMMAND ----------

display(telco_data_visual_dict[3])


# COMMAND ----------

display(telco_data_visual_dict[4])


# COMMAND ----------

display(telco_data_visual_dict[5])


# COMMAND ----------

display(telco_data_visual_dict[6])


# COMMAND ----------

display(telco_data_visual_dict[7])


# COMMAND ----------

#per il check per contare se si sono persi dati nel join
#for park in park_data_with_date_dict:
 # print(park_data_with_date_dict[park].count())

# COMMAND ----------

#nuovo, preparazione tabella dati regressione
all_tables = {} #un elemento per ogni parcheggio 
#fai il join aggiungendo tutti i dati di telecomunicazione da tutte le aree
for park in park_data_with_date_dict:
  temp = park_data_with_date_dict[park]
  for area in telco_data_with_date_dict:
    joined = temp.join(telco_data_with_date_dict[area],telco_data_with_date_dict[area].date == temp.date)
    joined = joined.drop(telco_data_with_date_dict[area].date)
    joined = joined.withColumnRenamed("count","count_"+str(area))
    temp = joined
  temp = temp.drop(temp.name)
  temp= temp.drop(temp.weekday)
  all_tables[park] = temp
  

# COMMAND ----------

weather_temp = weather.select(concat(year(weather.date),lit("-"),month(weather.date),lit("-"),dayofmonth(weather.date),lit("-"),hour(weather.date)).alias("date"),"temperature" )

for park in park_data_with_date_dict:
  temp = all_tables[park]
  joined = temp.join(weather_temp,weather_temp.date == temp.date)
  joined = joined.drop(weather_temp.date)
  all_tables[park] = joined

# COMMAND ----------

#ci sono ovviamente meno dati di prima (mancano dei dati metereologici! però non c'è nessun na, quindi significa che il join ha preso solo i dati buoni)
#ho controllato manualmente con il display, andata ...
#for park in park_data_with_date_dict:
 # print(all_tables[park].count(), all_tables[park].dropna().count())

# COMMAND ----------

features = ["temperature","count_1","count_2","count_3","count_4", "count_5", "count_6",  "count_7"]
for park in park_data_with_date_dict:
  all_tables[park] = all_tables[park].selectExpr("avgfree as label",*features) 

# COMMAND ----------

display(all_tables[park])
#seleziona scatterplot per vedere i plot a due a due

# COMMAND ----------

from itertools import combinations
columns = ["label","temperature","count_1","count_2","count_3","count_4", "count_5", "count_6",  "count_7"]
from pyspark.ml.stat import Correlation
for park in park_data_with_date_dict:
  print("###################################################")
  for comb in combinations(columns, 2):
    if(comb[0] == "label"):
      r1 = all_tables[park].stat.corr(comb[0],comb[1])
      print("correlation: park "+str(park)+", ("+comb[0]+","+comb[1]+"):"+str(r1))
  print("##############################################")
columns.remove("label")
#le correlazioni tra le altre features sono indipendenti dal parcheggio ...
for comb in combinations(columns,2):
  r1 = all_tables["109"].stat.corr(comb[0],comb[1])
  print("correlation: park "+str(park)+", ("+comb[0]+","+comb[1]+"):"+str(r1))
  print("##############################################")

# COMMAND ----------

'''{"Auguadri":("101",1),#Auguardi, area 1
         "Valduce":("104",1),#Valduce, area 1
         "Valmulini":("123",7),#Valmulini, area 7 (ma leggermente fuori mappa)
         "Castelnuovo":("107",7),#Castelnuovo, area 7
         "Centro Lago":("103",4),#Centro Lago, area 4
         "San Giovanni":("109",4),#San Giovanni, area 4
         "Sirtori":("105",1)     #Sirtori, confine area 1 e 7   
        }'''
#correlazione tra lable vs count negativa (come ci si poteva aspettare, visto che è free vs conta di persone presenti), e forte in zone adiacenti (eccetto per Sirtori, che in generale è molto poco usato come parcheggio, quindi produce correlazioni basse con tutti i count)
#correlazione label vs temperature trascurabile e spesso negativa
#correlazione temperatura vs count tutte molto deboli
#correlazione dei count tra di loro relativamente alte, anche in assenza di adiacenza (anche se di solito le più forti sono tra le adiacenti) <- questo potrebbe creare problemi (serve qualche forma di regolarizzazione durante la regressione!)

# COMMAND ----------

def saveModels(dict_models,path,typeofmodel):
  for park in park_data_with_date_dict:
    if typeofmodel == "linearRegression":
      dict_models[park].bestModel.save(path + str(park))
    elif typeofmodel == "tree" or typeofmodel == "gbt":
      dict_models[park].write().overwrite().save(path+str(park))
  

# COMMAND ----------

from pyspark.ml.regression import LinearRegressionModel, DecisionTreeRegressionModel, GBTRegressionModel
def loadModels(path,typeofmodel):
  models = {}
  for park in park_data_with_date_dict:
    if typeofmodel == "linear":
      models[park] = LinearRegressionModel.load(path+str(park))
    elif typeofmodel == "tree":
      models[park] = DecisionTreeRegressionModel.load(path+str(park))
    elif typeofmodel == "gbt":
      models[park] = GBTRegressionModel.load(path+str(park))
  return models

# COMMAND ----------

#il gbt nei casi più comuni ottiene dei residui maggiormente scorrelati e stazionari rispetto sia a tree che regressione -> buono
#per la sua alta flessibilità, il gbt tende ad apprendere il rumore sui dataset difficoltosi (e.g. quei dataset che a molti valori di variabili assegnano lo stesso valore y (e.g. i parcheggi quasi sempre vuoti ...)) <- in questi casi il modello meno flessibile (la regressione)si comporta meglio
#in generale, tutti e 3 i modelli mostrano R2 molto buoni (il gbt di solito ha la meglio)
#poiché la variabile tempo NON viene considerata (stiamo facendo predizione STATICA), il modello non riesce a catturare alcuni sbalzi di valore improvvisi (che sono fondamentalmente legati ad orari in cui i parcheggi si riempiono molto nell'arco di un'ora). 

# COMMAND ----------

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import LinearRegression
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit
from pyspark.ml.feature import VectorAssembler, StandardScaler

features = ["temperature","count_1","count_2","count_3","count_4", "count_5", "count_6",  "count_7"]

predictions = {}
models = {}
train_ds = {}
test_ds = {}

for park in park_data_with_date_dict:
  vectorAssembler = VectorAssembler(inputCols=features, outputCol="features")
  data = vectorAssembler.transform(all_tables[park])
  train, test = data.randomSplit([0.8,0.2], seed = 12345)
  train_ds[park] = train
  test_ds[park] = test
  
#stampa pure i summary
compute_again = False #cambiare questo parametro per rifare il calcolo, se è Falso carica i modelli già stimati
if compute_again == False:
  models = loadModels("LinearRegressionModel_","linear")
  for park in park_data_with_date_dict:
    predictions[park] = models[park].transform(test_ds[park])
else:
  for park in park_data_with_date_dict:
    #standardScaler = StandardScaler(inputCol="unscaled_features", outputCol="features")
    #pred4data = standardScaler.transform(pred4data)
    lr = LinearRegression(maxIter = 10)
    #elastic = forma ri regolarizzazione che mixa LASSO, e RIDGE
    paramGrid = ParamGridBuilder().addGrid(lr.regParam, [0.1, 0.01]).addGrid(lr.fitIntercept, [False, True]).addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0]).build() 
    tvs = TrainValidationSplit(estimator=lr,
                               estimatorParamMaps=paramGrid,
                               evaluator=RegressionEvaluator(),
                               # 80% of the data will be used for training, 20% for validation.
                               trainRatio=0.8)

    # Run TrainValidationSplit, and choose the best set of parameters.
    models[park] = tvs.fit(train_ds[park])
    # Make predictions on test data. model is the model with combination of parameters
    # that performed best.
    predictions[park] = models[park].transform(test_ds[park] )
  saveModels(models,"LinearRegressionModel_","linear")

# COMMAND ----------

#ATTENZIONE, la predizione è STATICA ... quindi anche se il plot è temporale, è solo per avere una visualizzazione "semplice" (altrimenti per fare un vero e proprio scatterplot variabili-predizione/valore vero ci vorrebbe una riduzione di dimensionalità ... ma mi sembra uno sforzo inutile)
def plotPredictions(code):
  fig, axes = plt.subplots()
  axes.plot([p.label for p in predictions[code].collect()], color = "blue")
  axes.plot([p.prediction for p in predictions[code].collect()], color = "red")
  #fig.autofmt_xdate()
  display(fig)

def printSummary(code):
  ''' #quando i modelli vengono salvati non vengono salvati i dati di training ... e diciamo che le informazioni di training sono abbasttanza relative ... meglio controllare che il modello funzioni sui nuovi dati e basta
  print("############OVER TRAINING")
  print("R2 = " + str(models[code].summary.r2))
  print("explainedVariance = " + str(models[code].summary.explainedVariance))
  print("meanSquaredError = " + str(models[code].summary.meanSquaredError))
  print("meanAbsoluteError = " + str(models[code].summary.meanAbsoluteError))
  print("rootMeanSquaredError = " + str(models[code].summary.rootMeanSquaredError))
  print("residuals = " + str(models[code].summary.residuals))
  '''
  if compute_again:
    print("############OVER TRAINING")
    print("R2 = " + str(models[code].bestModel.summary.r2))
    print("explainedVariance = " + str(models[code].bestModel.summary.explainedVariance))
    print("meanSquaredError = " + str(models[code].bestModel.summary.meanSquaredError))
    print("meanAbsoluteError = " + str(models[code].bestModel.summary.meanAbsoluteError))
    print("rootMeanSquaredError = " + str(models[code].bestModel.summary.rootMeanSquaredError))
    print("residuals = " + str(models[code].bestModel.summary.residuals))
    print("pvalues = " + str(models[code].bestModel.summary.pValues))
  else:
    print("\n ###############OVER TEST")
    eval = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")
    # Root Mean Square Error
    rmse = eval.evaluate(predictions[code])
    print("RMSE: %.3f" % rmse)
    # Mean Square Error
    mse = eval.evaluate(predictions[code], {eval.metricName: "mse"})
    print("MSE: %.3f" % mse)
    # Mean Absolute Error
    mae = eval.evaluate(predictions[code], {eval.metricName: "mae"})
    print("MAE: %.3f" % mae)
    # r2 - coefficient of determination
    r2 = eval.evaluate(predictions[code], {eval.metricName: "r2"})
    print("r2: %.3f" %r2)
    #print("pvalues = " + str(model.summary.pValues)) #non ci sono i pvalues perchè: https://stackoverflow.com/questions/46696378/spark-linearregressionsummary-normal-summary 
    print("coefficients: "+str(models[code].coefficients))
  
  
def plotResiduals(code):
  residual = predictions[code].selectExpr("prediction-label as res")
  fig, axes = plt.subplots()
  axes.plot([r.res for r in residual.collect()])
  #axes.plot([p.residuals for p in models[code].bestModel.summary.residuals.collect()])
  #axes.plot([p.residuals for p in models[code].summary.residuals.collect()])
  display(fig)

# COMMAND ----------

plotPredictions("109")

# COMMAND ----------

printSummary("109")
''' dal training set (li ricopio così poi si fa il load invece della stima le prossime volte)
############OVER TRAINING
R2 = 0.807932888043
explainedVariance = 921.978328647
meanSquaredError = 220.837703987
meanAbsoluteError = 9.64346804493
rootMeanSquaredError = 14.8606091392
residuals = DataFrame[residuals: double]
pvalues = [5.530504672890402e-06, 2.8377300509419e-13, 9.512390874988341e-13, 1.2820980277439276e-08, 0.0, 0.0, 0.0, 0.02057582336878272, 0.0]
'''

# COMMAND ----------

plotResiduals("109")
#ci sono delle "correlazioni" tra i residuals ... e c'è eteroschedasticità ... modello lineare non è il massimo, anche se R2 è buono

# COMMAND ----------

plotPredictions("101")


# COMMAND ----------

printSummary("101")
'''
############OVER TRAINING
R2 = 0.847364983591
explainedVariance = 25965.8045617
meanSquaredError = 4688.7030805
meanAbsoluteError = 44.6917223707
rootMeanSquaredError = 68.4741051822
residuals = DataFrame[residuals: double]
pvalues = [1.6404655411861313e-11, 0.0, 0.0, 3.110693372776474e-05, 1.475530808647818e-10, 0.0, 0.00033154716849947974, 0.027195805130485473, 0.0]'''

# COMMAND ----------

plotResiduals("101")

# COMMAND ----------

plotPredictions("104")

# COMMAND ----------

printSummary("104")
'''
############OVER TRAINING
R2 = 0.837735663034
explainedVariance = 9030.08112126
meanSquaredError = 1756.89041891
meanAbsoluteError = 29.1288066054
rootMeanSquaredError = 41.915276677
residuals = DataFrame[residuals: double]
pvalues = [0.0, 1.6380505380553245e-05, 0.0, 0.0, 0.15011108493347525, 4.440892098500626e-16, 0.08916606470051613, 0.0, 0.0]'''

# COMMAND ----------

plotResiduals("104")

# COMMAND ----------

plotPredictions("123")

# COMMAND ----------

printSummary("123")
'''
############OVER TRAINING
R2 = 0.76286764813
explainedVariance = 1154.30637509
meanSquaredError = 359.941897418
meanAbsoluteError = 13.7077410787
rootMeanSquaredError = 18.9721347618
residuals = DataFrame[residuals: double]
pvalues = [6.038321321066853e-05, 0.0, 0.0, 0.6243398053451665, 0.9831738779292434, 0.0, 0.0, 0.0, 0.0]'''

# COMMAND ----------

plotResiduals("123")

# COMMAND ----------

plotPredictions("107")



# COMMAND ----------

printSummary("107")
'''
############OVER TRAINING
R2 = 0.861208672097
explainedVariance = 6295.8022381
meanSquaredError = 1015.24613706
meanAbsoluteError = 23.3767229384
rootMeanSquaredError = 31.8629273146
residuals = DataFrame[residuals: double]
pvalues = [2.6546487097434124e-07, 0.0, 0.008832654900676973, 0.00011518510603969467, 0.03522064809409686, 2.007505273127208e-12, 0.0, 0.21487682917384743, 0.0]'''

# COMMAND ----------

plotResiduals("107")

# COMMAND ----------

plotPredictions("103")

# COMMAND ----------

printSummary("103")
'''
############OVER TRAINING
R2 = 0.80373792586
explainedVariance = 7780.1894335
meanSquaredError = 1900.42223994
meanAbsoluteError = 31.9325498145
rootMeanSquaredError = 43.5938325907
residuals = DataFrame[residuals: double]
pvalues = [3.156684201388593e-05, 0.03990366672444634, 0.0, 2.1106374403778716e-07, 0.04465425508424703, 0.0, 0.2555812836260969, 0.6385061665999772, 0.0]'''

# COMMAND ----------

plotResiduals("103")

# COMMAND ----------

plotPredictions("105")

# COMMAND ----------

printSummary("105")
'''
############OVER TRAINING
R2 = 0.117635037111
explainedVariance = 0.0174737157913
meanSquaredError = 0.154203720727
meanAbsoluteError = 0.230763054376
rootMeanSquaredError = 0.392687815863
residuals = DataFrame[residuals: double]
pvalues = [0.6305185505449238, 0.7644963057337328, 2.8625498362799817e-06, 0.45959198027631665, 0.0014119286066498482, 0.03755870913637516, 0.019161218324444507, 0.0005774178136548347, 0.0]
'''

# COMMAND ----------

plotResiduals("105")

# COMMAND ----------

#with a decision tree
from pyspark.ml.regression import DecisionTreeRegressor
dt_models = {}
dt_predictions = {}

compute_again = False 
if compute_again == False:
  dt_models = loadModels("TreeModel_","tree")
  for park in park_data_with_date_dict:
    dt_predictions[park] = dt_models[park].transform(test_ds[park])
else:
  for park in park_data_with_date_dict:
    #vectorAssembler = VectorAssembler(inputCols=features, outputCol="features")
    #data = vectorAssembler.transform(all_tables[park])
    #train, test = data.randomSplit([0.8,0.2], seed = 12345)
    dt = DecisionTreeRegressor()
    dt_models[park] = dt.fit(train_ds[park])
    dt_predictions[park] = dt_models[park].transform(test_ds[park])
  saveModels(dt_models,"TreeModel_","tree")

# COMMAND ----------

#ATTENZIONE: se vuoi visualizzare proprio gli alberi puoi chiamare display(dt_models[park])

# COMMAND ----------

def printEvaluateModel(park,modelsCollection, predictionsCollection):
  print("EVALUATE MODEL FOR PARKING "+str(park))
  print("OVER TEST SET")
  print("Features importance:" + str(modelsCollection[park].featureImportances))
  eval = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")
  # Root Mean Square Error
  rmse = eval.evaluate(predictionsCollection[park])
  print("RMSE: %.3f" % rmse)
  # Mean Square Error
  mse = eval.evaluate(predictionsCollection[park], {eval.metricName: "mse"})
  print("MSE: %.3f" % mse)
  # Mean Absolute Error
  mae = eval.evaluate(predictionsCollection[park], {eval.metricName: "mae"})
  print("MAE: %.3f" % mae)
  # r2 - coefficient of determination
  r2 = eval.evaluate(predictionsCollection[park], {eval.metricName: "r2"})
  print("r2: %.3f" %r2)  

# COMMAND ----------

def plotPredictionsT(code,predictionsCollection):
  fig, axes = plt.subplots()
  print(code)
  print(predictionsCollection[code])
  axes.plot([p.label for p in predictionsCollection[code].collect()], color = "blue")
  axes.plot([p.prediction for p in predictionsCollection[code].collect()], color = "red")
  #fig.autofmt_xdate()
  display(fig)

# COMMAND ----------

def plotResidualsT(code,predictions,typeofpred):
  if typeofpred == "tree":
    residual = dt_predictions[code].selectExpr("prediction-label as res")
  elif typeofpred == "gbt":
    residual = gbt_predictions[code].selectExpr("prediction-label as res")
  fig, axes = plt.subplots()
  axes.plot([r.res for r in residual.collect()])
  fig.autofmt_xdate()
  display(fig)

# COMMAND ----------

printEvaluateModel("101",dt_models, dt_predictions)
#in area 1, count_2 molto forte (ciò è sensato, visto che è al confine tra 1 e 2 e l'unico altro parcheggio in area 2 è molto affollato)
'''
o	“101”-Aguadri: forte importanza di count_2 (giustificabile, visto che il parcheggio è al confine tra area 1 e 2, e la zona 2 è abbastanza affollata di solito)
'''

# COMMAND ----------

plotPredictionsT("101", dt_predictions)

# COMMAND ----------

plotResidualsT("101", dt_predictions,"tree")

# COMMAND ----------

printEvaluateModel("104",dt_models, dt_predictions)
'''
o	“104”-Valduce: di nuovo forte importanza di count_2, stesse considerazioni di “101”
'''

# COMMAND ----------

plotPredictionsT("104", dt_predictions)

# COMMAND ----------

plotResidualsT("104", dt_predictions,"tree")

# COMMAND ----------

printEvaluateModel("123",dt_models, dt_predictions)
'''
o	“123”-Valmulini: forte importanza di count_1 e discreta rilevanza di count_6. La seconda potrebbe essere spuria, la prima ha senso (zona 7 e 1 sono adiacenti, e spesso 1 è più affollata di 7, quindi le persone potrebbero parcheggiare in 7 e passeggiare fino ad 1).
'''

# COMMAND ----------

plotPredictionsT("123", dt_predictions)

# COMMAND ----------

plotResidualsT("123", dt_predictions,"tree")

# COMMAND ----------

printEvaluateModel("107",dt_models, dt_predictions)
'''
o	“107”-Castelnuovo: forte importanza count_1 e relativamente count_6 (stesse considerazioni di “123”)
'''

# COMMAND ----------

plotPredictionsT("107", dt_predictions)

# COMMAND ----------

plotResidualsT("107", dt_predictions,"tree")

# COMMAND ----------

printEvaluateModel("103",dt_models, dt_predictions)
'''
o	“103”-ComoLago: forte importanza di count_2 e count_3 (ha senso, visto che sono zone di lago, e comolago è il parcheggio “più vicino” alla sponda est del lago)
'''

# COMMAND ----------

plotPredictionsT("103", dt_predictions)

# COMMAND ----------

plotResidualsT("103", dt_predictions,"tree")

# COMMAND ----------

printEvaluateModel("109",dt_models, dt_predictions)
'''
o	“109”-San Giovanni: forte importanza di count_1 e count_6 (count_1 è naturale, count_6 potrebbe essere spurio)
'''

# COMMAND ----------

plotPredictionsT("109", dt_predictions)

# COMMAND ----------

plotResidualsT("109", dt_predictions,"tree")

# COMMAND ----------

printEvaluateModel("105",dt_models, dt_predictions)
'''
o	“105”-Sirtori: forte count_7, e relativamente count_3, count_6 e temperatura. Il primo ha molto senso, gli altri due count potrebbero essere spuri, così anche la temperatura (anche se la presenza di parcheggi all’aperto lì vicino potrebbe spiegare la cosa)
'''

# COMMAND ----------

plotPredictionsT("105", dt_predictions)

# COMMAND ----------

plotResidualsT("105", dt_predictions,"tree")

# COMMAND ----------

l'albero è particolarmente articolato e di difficile interpretazione, e siamo più interessati a predizioni e importanza di features, più che alla struttura in sè
display(dt_models["103"])

# COMMAND ----------

from pyspark.ml.regression import GBTRegressor
gbt_models = {}
gbt_predictions = {}

compute_again = False 
if compute_again == False:
  gbt_models = loadModels("gbtModel_","gbt")
  for park in park_data_with_date_dict:
    gbt_predictions[park] = gbt_models[park].transform(test_ds[park])
else:
  for park in park_data_with_date_dict:
    #vectorAssembler = VectorAssembler(inputCols=features, outputCol="features")
    #data = vectorAssembler.transform(all_tables[park])
    #train, test = data.randomSplit([0.8,0.2], seed = 12345)
    gbt = GBTRegressor(maxIter=10)
    gbt_models[park] = gbt.fit(train_ds[park])
    gbt_predictions[park] = gbt_models[park].transform(test_ds[park])
  saveModels(gbt_models,"gbtModel_","gbt")

# COMMAND ----------

printEvaluateModel("101",gbt_models, gbt_predictions)
'''
o	“101”-Aguadri: l’importanza è più o meno distribuita equamente, con picchi in temperature, count_2 e count_7 (che potrebbe avere senso, vista la vicinanza all’area 7)
'''

# COMMAND ----------

plotPredictionsT("101", gbt_predictions)

# COMMAND ----------

plotResidualsT("101", gbt_predictions,"gbt")

# COMMAND ----------

printEvaluateModel("104",gbt_models, gbt_predictions)
'''
o	“104”-Valduce: l’importanza maggiore è di temperature, count_2, count_7 e count_3 (tutti abbastanza plausibili)
'''


# COMMAND ----------

plotPredictionsT("104", gbt_predictions)


# COMMAND ----------

plotResidualsT("104", gbt_predictions,"gbt")

# COMMAND ----------

printEvaluateModel("123",gbt_models, gbt_predictions)
'''
o	“123”-Valmulini: l’importanza è più su count_1,  count_5 e count_6. Le ultime due potrebbero essere spurie. L’importanza è comunque distributa in modo relativamente omogeneo. 
'''

# COMMAND ----------

plotPredictionsT("123", gbt_predictions)


# COMMAND ----------

plotResidualsT("123",gbt_predictions,"gbt")

# COMMAND ----------

printEvaluateModel("107",gbt_models, gbt_predictions)
'''
o	“107”-Castelnuovo: importanti count_1, count_5 e count_6 (6 potrebbe essere spuria, count_5 forse potrebbe essere plausibile)
'''

# COMMAND ----------

plotPredictionsT("107", gbt_predictions)

# COMMAND ----------

plotResidualsT("107", gbt_predictions,"gbt")

# COMMAND ----------

printEvaluateModel("103",gbt_models, gbt_predictions)
'''
o	“103”-Como Lago: importanti count_3, count_2, count_5 e count_7 (tutte moltoplausibili)
'''

# COMMAND ----------

plotPredictionsT("103", gbt_predictions)

# COMMAND ----------

plotResidualsT("103", gbt_predictions,"gbt")

# COMMAND ----------

printEvaluateModel("105",gbt_models, gbt_predictions)
'''
o	“105”- Sirtori: importanti temperature, count_6 e poi count_1/3/4/7  (probabilmente i legami con i count sono spuri)
'''

# COMMAND ----------

plotPredictionsT("105", gbt_predictions)

# COMMAND ----------

plotResidualsT("105", gbt_predictions,"gbt")

# COMMAND ----------

printEvaluateModel("109",gbt_models, gbt_predictions)
'''
o	“109”- San Giovanni: forte temperatura e count_1 e count_4 (plausibili), e count_2 e count_5 (meno plausibili)
'''

# COMMAND ----------

plotPredictionsT("109", gbt_predictions)

# COMMAND ----------

plotResidualsT("109", gbt_predictions,"gbt")

# COMMAND ----------

#VOLENDO potrei plottare le regole di decisione o mostrare gli alberi
display(dt_models["109"])
#print(dt_models["109"].toDebugString)
#print(gbt_models["109"].toDebugString)

# COMMAND ----------


