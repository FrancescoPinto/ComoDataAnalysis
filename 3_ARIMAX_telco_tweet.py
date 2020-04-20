# Databricks notebook source
from pyspark.sql.types import DateType, IntegerType, StructField
def fromPandasDFToDF(pdf,timestamp_column_name):
 # rdd = sc.parallelize(pdf).map(lambda x:(x[0],x[1]))
 # schema = StructType([StructField("date", DateType(), True), StructField("count", IntegerType(),True)])
  df = spark.createDataFrame(pdf)#schema=schema)
  df_with_date = df.withColumn(timestamp_column_name,from_unixtime(unix_timestamp(timestamp_column_name, 'HH:mm - dd MM yyyy')).cast("date")) #puoi anche usare "timestamp" per conservare anche l'ora, ma per ora non ti serve
  return df_with_date

def fromPandasSeriesToPandasDF(series):
  temp = series.to_frame()
  temp = temp.reset_index()
  return temp

# COMMAND ----------

df = spark.read.format("csv").option("header", "true").option("multiline",'true').option("inferSchema", 'true').load("dbfs:/FileStore/tables/AllQuery3Tweets2017.csv")
print(df.count())
df = df.dropna()
df = df.na.fill("")
#get Date instead of String for Timestamp
from pyspark.sql.functions import *
df_with_date = df.withColumn("timestamp",from_unixtime(unix_timestamp('timestamp', 'HH:mm - dd MM yyyy')).cast("date")) #puoi anche usare "timestamp" per conservare anche l'ora, ma per ora non ti serve
df_with_date.createOrReplaceTempView("df_with_date")
df = spark.sql("select timestamp, count(*) as total from df_with_date group by timestamp order by timestamp asc")
df.count()

# COMMAND ----------

display(df)

# COMMAND ----------

#passo a pandas perché:
# è più facile manipolare le date se ho gli indici
# non ci sono librerie spark che gestiscono le serie temporali (che io sappia) BEN SUPPORTATE DA DataBricks
import pandas as pd
pandas_df = df.toPandas()
pandas_df["timestamp"] = pd.to_datetime(pandas_df["timestamp"], format = "%Y-%m-%d")
pandas_df.dtypes

# COMMAND ----------

pandas_df = pandas_df.set_index('timestamp')
pandas_df


# COMMAND ----------

#now we can perform advanced indexing <- e.g. "2016-03":"2016-05" o più completi
pandas_df["2016-03"]

# COMMAND ----------

#convert to Series, to avoid the need of referring to columns when using dataframe
pandas_series= pandas_df["total"]
pandas_series

# COMMAND ----------

#CONTROLLO SE LA CONVERSIONE INVERSA E' ANDATA A BUON FINE
display(fromPandasDFToDF(fromPandasSeriesToPandasDF(pandas_series), "timestamp"))
'''import matplotlib.pyplot as plt
fig, axes = plt.subplots()
axes.plot(pandas_series)
fig.autofmt_xdate()
display(fig)'''

# COMMAND ----------

#mostra un lieve trending

# COMMAND ----------

#check stationarity
from statsmodels.tsa.stattools import adfuller
from pyspark.sql.functions import lit

import pandas as pd
def visualize_stationarity(timeseries, timestamp_column_name):
    #Determing rolling statistics
    rolmean = pd.rolling_mean(timeseries, window=14)
    rolstd = pd.rolling_std(timeseries, window=14)
    
    pyDF_timeseries = fromPandasDFToDF(fromPandasSeriesToPandasDF(timeseries), timestamp_column_name)
    pyDF_rolmean = fromPandasDFToDF(fromPandasSeriesToPandasDF(rolmean), timestamp_column_name)
    pyDF_rolstd = fromPandasDFToDF(fromPandasSeriesToPandasDF(rolstd), timestamp_column_name)
    
    pyDF_timeseries = pyDF_timeseries.withColumn("type",lit("count"))
    pyDF_rolmean = pyDF_rolmean.withColumn("type",lit("rolmean"))
    pyDF_rolstd = pyDF_rolstd.withColumn("type",lit("rolstd"))
    pyDF_all = pyDF_timeseries.union(pyDF_rolmean).union(pyDF_rolstd)
    display(pyDF_all)
    #Plot rolling statistics:
    '''fig, axes = plt.subplots()
    axes.plot(timeseries, color='blue',label='Original')
    axes.plot(rolmean, color='red', label='Rolling Mean')
    axes.plot(rolstd, color='black', label = 'Rolling Std')
    plt.legend(loc='best')
    plt.title('Rolling Mean & Standard Deviation')
    fig.autofmt_xdate()
    display(fig)'''
def test_stationarity(timeseries):
    #Perform Dickey-Fuller test:
    #REMARK: null hypothesis = unit root -> nonstationarity
    #se p-value basso allora rifiuto e quindi è stazionario
    print 'Results of Dickey-Fuller Test:'
    dftest = adfuller(timeseries, autolag='AIC')
    dfoutput = pd.Series(dftest[0:4], index=['Test Statistic','p-value','#Lags Used','Number of Observations Used'])
    for key,value in dftest[4].items():
        dfoutput['Critical Value (%s)'%key] = value
    print dfoutput
    
visualize_stationarity(pandas_series,"timestamp")


# COMMAND ----------

test_stationarity(pandas_series)
#h0 = has unit root (ricorda, radice su cerchio unitario rende instabile la serie)
#il test di stazionarietà ci dice che in realtà è stazionario ...

# COMMAND ----------

#ACF and PACF plots:
from statsmodels.tsa.stattools import acf, pacf
import numpy as np
import matplotlib.pyplot as plt 
def chooseARIMAXOrder(pandas_series):
  lag_acf = acf(pandas_series, nlags=20)
  lag_pacf = pacf(pandas_series, nlags=20, method='ols')

  fig, axes = plt.subplots(2,1)

  #Plot ACF: #acf = autocorrelation function
  axes[0].plot(lag_acf)
  axes[0].axhline(y=0,linestyle='--',color='gray')
  axes[0].axhline(y=-1.96/np.sqrt(len(pandas_series)),linestyle='--',color='gray')
  axes[0].axhline(y=1.96/np.sqrt(len(pandas_series)),linestyle='--',color='gray')
  axes[0].title.set_text("ACF, choose q")
  #Plot PACF: #pacf = partial autocorrelation function
  axes[1].plot(lag_pacf)
  axes[1].axhline(y=0,linestyle='--',color='blue')
  axes[1].axhline(y=-1.96/np.sqrt(len(pandas_series)),linestyle='--',color='blue')
  axes[1].axhline(y=1.96/np.sqrt(len(pandas_series)),linestyle='--',color='blue')
  axes[1].title.set_text("PACF, choose p")

  display(fig)
#hence
#p = lag value where PACF chart first corrses the upper confidence interval
#q = " ACF "


# COMMAND ----------

chooseARIMAXOrder(pandas_series)
#p = 3
#q = 4

# COMMAND ----------

pandas_series = pandas_series.astype(np.float64)
pandas_series.dtypes

# COMMAND ----------

from statsmodels.tsa.arima_model import ARIMA

#try an AR
#                           AR  I  MA
model = ARIMA(pandas_series, order=(3, 0, 0))  
results_AR = model.fit(disp=-1)  
pyDF_timeseries = fromPandasDFToDF(fromPandasSeriesToPandasDF(pandas_series), "timestamp")
pyDF_results_AR = fromPandasDFToDF(fromPandasSeriesToPandasDF(results_AR.fittedvalues), "timestamp")
pyDF_timeseries = pyDF_timeseries.withColumn("type",lit("count"))
pyDF_results_AR = pyDF_results_AR.withColumn("type",lit("AR model"))
pyDF_all = pyDF_timeseries.union(pyDF_results_AR)
display(pyDF_all)

# COMMAND ----------

print('RSS: %.4f'% np.sum((results_AR.fittedvalues-pandas_series)**2))

# COMMAND ----------

#try a MA
model = ARIMA(pandas_series, order=(0, 0, 4))  
results_MA = model.fit(disp=-1)  
pyDF_timeseries = fromPandasDFToDF(fromPandasSeriesToPandasDF(pandas_series), "timestamp")
pyDF_results_MA = fromPandasDFToDF(fromPandasSeriesToPandasDF(results_MA.fittedvalues), "timestamp")
pyDF_timeseries = pyDF_timeseries.withColumn("type",lit("count"))
pyDF_results_MA = pyDF_results_MA.withColumn("type",lit("MA model"))
pyDF_all = pyDF_timeseries.union(pyDF_results_MA)
display(pyDF_all)
'''fig, axes = plt.subplots()
axes.plot(pandas_series)
axes.plot(results_MA.fittedvalues, color='red')
plt.title('RSS: %.4f'% np.sum((results_MA.fittedvalues-pandas_series)**2))
display(fig)'''

# COMMAND ----------

print('RSS: %.4f'% np.sum((results_MA.fittedvalues-pandas_series)**2))

# COMMAND ----------

#try ARIMA
model = ARIMA(pandas_series, order=(3, 0,4))  
results_ARIMA = model.fit(disp=-1)
pyDF_timeseries = fromPandasDFToDF(fromPandasSeriesToPandasDF(pandas_series), "timestamp")
pyDF_results_ARIMA = fromPandasDFToDF(fromPandasSeriesToPandasDF(results_ARIMA.fittedvalues), "timestamp")
pyDF_timeseries = pyDF_timeseries.withColumn("type",lit("count"))
pyDF_results_ARIMA = pyDF_results_ARIMA.withColumn("type",lit("ARIMA model"))
pyDF_all = pyDF_timeseries.union(pyDF_results_ARIMA)
display(pyDF_all)
'''
fig, axes = plt.subplots()
axes.plot(pandas_series)
axes.plot(results_ARIMA.fittedvalues, color='red')
plt.title('RSS: %.4f'% np.sum((results_ARIMA.fittedvalues-pandas_series)**2))
'''

# COMMAND ----------

import numpy as np
from scipy import signal
from scipy.signal.signaltools import _centered as trim_centered

def _pad_nans(x, head=None, tail=None):
    if np.ndim(x) == 1:
        if head is None and tail is None:
            return x
        elif head and tail:
            return np.r_[[np.nan] * head, x, [np.nan] * tail]
        elif tail is None:
            return np.r_[[np.nan] * head, x]
        elif head is None:
            return np.r_[x, [np.nan] * tail]
    elif np.ndim(x) == 2:
        if head is None and tail is None:
            return x
        elif head and tail:
            return np.r_[[[np.nan] * x.shape[1]] * head, x,
                         [[np.nan] * x.shape[1]] * tail]
        elif tail is None:
            return np.r_[[[np.nan] * x.shape[1]] * head, x]
        elif head is None:
            return np.r_[x, [[np.nan] * x.shape[1]] * tail]
    else:
        raise ValueError("Nan-padding for ndim > 2 not implemented")
        
def convolution_filter(x, filt, nsides=2):
    # for nsides shift the index instead of using 0 for 0 lag this
    # allows correct handling of NaNs
    if nsides == 1:
        trim_head = len(filt) - 1
        trim_tail = None
    elif nsides == 2:
        trim_head = int(np.ceil(len(filt)/2.) - 1) or None
        trim_tail = int(np.ceil(len(filt)/2.) - len(filt) % 2) or None
    else:  # pragma : no cover
        raise ValueError("nsides must be 1 or 2")

    _pandas_wrapper = _maybe_get_pandas_wrapper(x)
    x = np.asarray(x)
    filt = np.asarray(filt)
    if x.ndim > 1 and filt.ndim == 1:
        filt = filt[:, None]
    if x.ndim > 2:
        raise ValueError('x array has to be 1d or 2d')

    if filt.ndim == 1 or min(filt.shape) == 1:
        result = signal.convolve(x, filt, mode='valid')
    elif filt.ndim == 2:
        nlags = filt.shape[0]
        nvar = x.shape[1]
        result = np.zeros((x.shape[0] - nlags + 1, nvar))
        if nsides == 2:
            for i in range(nvar):
                # could also use np.convolve, but easier for swiching to fft
                result[:, i] = signal.convolve(x[:, i], filt[:, i],
                                               mode='valid')
        elif nsides == 1:
            for i in range(nvar):
                result[:, i] = signal.convolve(x[:, i], np.r_[0, filt[:, i]],
                                               mode='valid')
    result = _pad_nans(result, trim_head, trim_tail)
    if _pandas_wrapper:
        return _pandas_wrapper(result)
    return result

# COMMAND ----------


from statsmodels.compat.python import lmap, range, iteritems
import numpy as np
from pandas.core.nanops import nanmean as pd_nanmean
from statsmodels.tsa.filters._utils import (_maybe_get_pandas_wrapper_freq,
                             _maybe_get_pandas_wrapper)
#from statsmodels.tsa.filters.filtertools import convolution_filter
from statsmodels.tsa.tsatools import freq_to_period


def seasonal_mean(x, freq):
    """
    Return means for each period in x. freq is an int that gives the
    number of periods per cycle. E.g., 12 for monthly. NaNs are ignored
    in the mean.
    """
    return np.array([pd_nanmean(x[i::freq], axis=0) for i in range(freq)])


def _extrapolate_trend(trend, npoints):
    """
    Replace nan values on trend's end-points with least-squares extrapolated
    values with regression considering npoints closest defined points.
    """
    front = next(i for i, vals in enumerate(trend)
                 if not np.any(np.isnan(vals)))
    back = trend.shape[0] - 1 - next(i for i, vals in enumerate(trend[::-1])
                                     if not np.any(np.isnan(vals)))
    front_last = min(front + npoints, back)
    back_first = max(front, back - npoints)

    k, n = np.linalg.lstsq(
        np.c_[np.arange(front, front_last), np.ones(front_last - front)],
        trend[front:front_last], rcond=-1)[0]
    extra = (np.arange(0, front) * np.c_[k] + np.c_[n]).T
    if trend.ndim == 1:
        extra = extra.squeeze()
    trend[:front] = extra

    k, n = np.linalg.lstsq(
        np.c_[np.arange(back_first, back), np.ones(back - back_first)],
        trend[back_first:back], rcond=-1)[0]
    extra = (np.arange(back + 1, trend.shape[0]) * np.c_[k] + np.c_[n]).T
    if trend.ndim == 1:
        extra = extra.squeeze()
    trend[back + 1:] = extra

    return trend


def seasonal_decompose(x, model="additive", filt=None, freq=None, two_sided=True,
                       extrapolate_trend=0):
    if freq is None:
        _pandas_wrapper, pfreq = _maybe_get_pandas_wrapper_freq(x)
    else:
        _pandas_wrapper = _maybe_get_pandas_wrapper(x)
        pfreq = None
    x = np.asanyarray(x).squeeze()
    nobs = len(x)

    if not np.all(np.isfinite(x)):
        raise ValueError("This function does not handle missing values")
    if model.startswith('m'):
        if np.any(x <= 0):
            raise ValueError("Multiplicative seasonality is not appropriate "
                             "for zero and negative values")

    if freq is None:
        if pfreq is not None:
            pfreq = freq_to_period(pfreq)
            freq = pfreq
        else:
            raise ValueError("You must specify a freq or x must be a "
                             "pandas object with a timeseries index with "
                             "a freq not set to None")

    if filt is None:
        if freq % 2 == 0:  # split weights at ends
            filt = np.array([.5] + [1] * (freq - 1) + [.5]) / freq
        else:
            filt = np.repeat(1./freq, freq)

    nsides = int(two_sided) + 1
    trend = convolution_filter(x, filt, nsides)

    if extrapolate_trend == 'freq':
        extrapolate_trend = freq - 1

    if extrapolate_trend > 0:
        trend = _extrapolate_trend(trend, extrapolate_trend + 1)

    if model.startswith('m'):
        detrended = x / trend
    else:
        detrended = x - trend

    period_averages = seasonal_mean(detrended, freq)

    if model.startswith('m'):
        period_averages /= np.mean(period_averages, axis=0)
    else:
        period_averages -= np.mean(period_averages, axis=0)

    seasonal = np.tile(period_averages.T, nobs // freq + 1).T[:nobs]

    if model.startswith('m'):
        resid = x / seasonal / trend
    else:
        resid = detrended - seasonal

    results = lmap(_pandas_wrapper, [seasonal, trend, resid, x])
    return DecomposeResult(seasonal=results[0], trend=results[1],
                           resid=results[2], observed=results[3])



class DecomposeResult(object):
    def __init__(self, **kwargs):
        for key, value in iteritems(kwargs):
            setattr(self, key, value)
        self.nobs = len(self.observed)

    def plot(self):
        from statsmodels.graphics.utils import _import_mpl
        plt = _import_mpl()
        fig, axes = plt.subplots(4, 1, sharex=True)
        if hasattr(self.observed, 'plot'):  # got pandas use it
            self.observed.plot(ax=axes[0], legend=False)
            axes[0].set_ylabel('Observed')
            self.trend.plot(ax=axes[1], legend=False)
            axes[1].set_ylabel('Trend')
            self.seasonal.plot(ax=axes[2], legend=False)
            axes[2].set_ylabel('Seasonal')
            self.resid.plot(ax=axes[3], legend=False)
            axes[3].set_ylabel('Residual')
        else:
            axes[0].plot(self.observed)
            axes[0].set_ylabel('Observed')
            axes[1].plot(self.trend)
            axes[1].set_ylabel('Trend')
            axes[2].plot(self.seasonal)
            axes[2].set_ylabel('Seasonal')
            axes[3].plot(self.resid)
            axes[3].set_ylabel('Residual')
            axes[3].set_xlabel('Time')
            axes[3].set_xlim(0, self.nobs)

        fig.tight_layout()
        return fig

# COMMAND ----------

#from statsmodels.tsa.seasonal import seasonal_decompose
decomposition_tweet = seasonal_decompose(pandas_series.values, freq =7)
#devi fare questo (ovvero fare l'ARMA della sola time series (come hai già fatto per ARIMAX))

# COMMAND ----------

trend_tweet = decomposition_tweet.trend
fig, axes = plt.subplots()
axes.plot(trend_tweet,color='red',label='trend')
axes.plot(pandas_series.values, color = "blue", label="original")
plt.legend(loc='best')
display(fig)

# COMMAND ----------

seasonal_tweet = decomposition_tweet.seasonal
fig, axes = plt.subplots()
axes.plot(seasonal_tweet,color='red',label='seasonal')
axes.plot(pandas_series.values, color = "blue", label="original")
plt.legend(loc='best')
display(fig)

# COMMAND ----------

#ATTENZIONE: residual != i residui della regressione, bensì è ciò che rimane dopo aver rimosso la non stazionarietà!!!!
residual_tweet = decomposition_tweet.resid
fig, axes = plt.subplots()
axes.plot(residual_tweet,color='red',label='residual')
axes.plot(pandas_series.values, color = "blue", label="original")
plt.legend(loc='best')
display(fig)

# COMMAND ----------

def from_list_to_time_indexed_visualization(list_in,low_time, high_time):
  my_list = list_in.tolist()
  rng = pd.date_range(low_time,high_time, freq='D')
  res = pd.Series(my_list, index = rng)
  res = res.dropna()
  res.index.name = "timestamp"
  #ser[0].name = "count"
  visualize_stationarity(res,"timestamp")
  return res

# COMMAND ----------

res_tweet = from_list_to_time_indexed_visualization(residual_tweet, '2016-01-01', '2016-12-31')

# COMMAND ----------

test_stationarity(res_tweet)
#dovremmo essere piuttosto confidenti ora

# COMMAND ----------

chooseARIMAXOrder(res_tweet)
#p = q = 1

# COMMAND ----------

#visualizzo la predizione del rumore
model = ARIMA(res_tweet, order=(1, 0,1))  
results_ARIMA = model.fit(disp=-1)
pyDF_timeseries = fromPandasDFToDF(fromPandasSeriesToPandasDF(res_tweet), "timestamp")
pyDF_results_ARIMA = fromPandasDFToDF(fromPandasSeriesToPandasDF(results_ARIMA.fittedvalues), "timestamp")
pyDF_timeseries = pyDF_timeseries.withColumn("type",lit("count"))
pyDF_results_ARIMA = pyDF_results_ARIMA.withColumn("type",lit("ARIMA model"))
pyDF_all = pyDF_timeseries.union(pyDF_results_ARIMA)
display(pyDF_all)

# COMMAND ----------

print('RSS: %.4f'% np.sum((results_ARIMA.fittedvalues-res_tweet)**2))
#abbiamo più che dimezzato l'RSS!

# COMMAND ----------

trend_df_tweet = from_list_to_time_indexed_visualization(trend_tweet, '2016-01-01', '2016-12-31')

# COMMAND ----------

seasonal_df_tweet = from_list_to_time_indexed_visualization(seasonal_tweet, '2016-01-01', '2016-12-31')


# COMMAND ----------

rebuilt = results_ARIMA.fittedvalues.add(trend_df_tweet).add(seasonal_df_tweet)
rebuilt

# COMMAND ----------

#riaggiungendo le non stazionarietà, com si può vedere le prestazioni sono molto buone!
pyDF_timeseries = fromPandasDFToDF(fromPandasSeriesToPandasDF(pandas_series), "timestamp")
pyDF_results_ARIMA = fromPandasDFToDF(fromPandasSeriesToPandasDF(rebuilt), "timestamp")
pyDF_timeseries = pyDF_timeseries.withColumn("type",lit("count"))
pyDF_results_ARIMA = pyDF_results_ARIMA.withColumn("type",lit("ARIMA model"))
pyDF_all = pyDF_timeseries.union(pyDF_results_ARIMA)
display(pyDF_all)

# COMMAND ----------

print('RSS: %.4f'% np.sum((rebuilt-pandas_series)**2))


# COMMAND ----------

print(results_ARIMA.summary())
#dagli intervalli di confidenza al 95% abbiamo che il valore vero dei due coefficienti dovrebbe essere NON nullo (uno positivo, l'altro negativo)

# COMMAND ----------

from pandas import DataFrame
# plot residual errors
residuals = DataFrame(results_ARIMA.resid)
fig, axes = plt.subplots()
axes.plot(results_ARIMA.resid,color='red',label='residual')
fig.autofmt_xdate()
display(fig)


# COMMAND ----------

from scipy.stats import shapiro
stat, p = shapiro(residuals)
print(stat,p)
# interpret: gaussian residuals, ok!

# COMMAND ----------

print(residuals.describe())

# COMMAND ----------

#I risultati a se stanti non sono poi così buoni ... proviamo ora ad introdurre delle variabili esogene
telco_count_daily_city = spark.read.format("csv").option("header", "true").option("multiline",'true').option("inferSchema", 'true').load("dbfs:/FileStore/tables/count_daily_city.csv")
telco_count_daily_city.createOrReplaceTempView("telco_count_city")
telco_df = spark.sql("select * from telco_count_city order by date")
display(telco_df)

# COMMAND ----------

import pandas as pd
telco_pandas_df = telco_df.toPandas()
telco_pandas_df["timestamp"] = pd.to_datetime(telco_pandas_df["date"], format = "%Y-%m-%d")
telco_pandas_df = telco_pandas_df.set_index('date')
telco_pandas_series= telco_pandas_df["count"]
telco_pandas_series = telco_pandas_series["2016-06":"2016-10"]
telco_pandas_series

# COMMAND ----------

visualize_stationarity(telco_pandas_series, "date")


# COMMAND ----------

test_stationarity(telco_pandas_series)
#h0 = has unit root (ricorda, radice su cerchio unitario rende instabile la serie)
#in teoria p-value > 0.05 quindi rifiutiamo (ma è eclatante)

# COMMAND ----------

#from statsmodels.tsa.seasonal import seasonal_decompose
decomposition_telco = seasonal_decompose(telco_pandas_series.values, freq =7)
#credo il problema sia legato agli indici vedi pd.date_range

# COMMAND ----------

trend_telco = decomposition_telco.trend
fig, axes = plt.subplots()
axes.plot(trend_telco,color='red',label='trend')
axes.plot(telco_pandas_series.values, color = "blue", label="original")
plt.legend(loc='best')
display(fig)

# COMMAND ----------

seasonal_telco = decomposition_telco.seasonal
fig, axes = plt.subplots()
axes.plot(seasonal_telco,color='red',label='seasonal')
axes.plot(telco_pandas_series.values, color = "blue", label="original")
plt.legend(loc='best')
display(fig)

# COMMAND ----------

#ATTENZIONE: residual != i residui della regressione, bensì è ciò che rimane dopo aver rimosso la non stazionarietà!!!!
residual_telco = decomposition_telco.resid
fig, axes = plt.subplots()
axes.plot(residual_telco,color='red',label='residual')
axes.plot(telco_pandas_series.values, color = "blue", label="original")
plt.legend(loc='best')
display(fig)

# COMMAND ----------

len(residual_telco)

# COMMAND ----------

res_telco = from_list_to_time_indexed_visualization(residual_telco, '2016-06-01', '2016-10-30')

# COMMAND ----------

test_stationarity(res_telco)
#stazionario

# COMMAND ----------

partial_res_tweet = res_tweet['2016-06-04':'2016-10-27']

# COMMAND ----------

model_X = ARIMA(partial_res_tweet, order=(1, 0,1),exog = res_telco)  
results_ARIMAX = model_X.fit(disp=-1)
pyDF_timeseries = fromPandasDFToDF(fromPandasSeriesToPandasDF(partial_res_tweet), "timestamp")
pyDF_results_ARIMAX = fromPandasDFToDF(fromPandasSeriesToPandasDF(results_ARIMAX.fittedvalues), "timestamp")
pyDF_timeseries = pyDF_timeseries.withColumn("type",lit("count"))
pyDF_results_ARIMAX = pyDF_results_ARIMAX.withColumn("type",lit("ARIMAX model"))
pyDF_all = pyDF_timeseries.union(pyDF_results_ARIMAX)
display(pyDF_all)

# COMMAND ----------

print('RSS: %.4f'% np.sum((results_ARIMAX.fittedvalues-partial_res_tweet)**2))


# COMMAND ----------

#ricostruiamo il segnal dei tweet usando seasonal e trend per visualizzare meglio
rebuilt = results_ARIMAX.fittedvalues.add(trend_df_tweet['2016-06-04':'2016-10-27']).add(seasonal_df_tweet['2016-06-04':'2016-10-27'])
rebuilt

# COMMAND ----------

#riaggiungendo le non stazionarietà, com si può vedere le prestazioni sono molto buone!
pyDF_timeseries = fromPandasDFToDF(fromPandasSeriesToPandasDF(pandas_series['2016-06-04':'2016-10-27']), "timestamp")
pyDF_results_ARIMAX = fromPandasDFToDF(fromPandasSeriesToPandasDF(rebuilt), "timestamp")
pyDF_timeseries = pyDF_timeseries.withColumn("type",lit("count"))
pyDF_results_ARIMAX = pyDF_results_ARIMAX.withColumn("total",col("0")).drop("0").withColumn("type",lit("ARIMA model"))
pyDF_all = pyDF_timeseries.union(pyDF_results_ARIMAX)
display(pyDF_all)

# COMMAND ----------

print('RSS: %.4f'% np.sum((rebuilt-pandas_series['2016-06-04':'2016-10-27'])**2))


# COMMAND ----------

s1 = pd.Series(res_telco, name='telco')
s2 = pd.Series(partial_res_tweet, name='tweet')
ttt = pd.concat([s2, s1], axis=1)

# COMMAND ----------

from statsmodels.tsa.stattools import grangercausalitytests
results = grangercausalitytests(x=ttt,maxlag=10) #col2 predice col1
#null hypothesis = X does not cause Y 
#quindi, p-value >> 0.1 = null hypothesis cannot be rejected
#NOTA: più cresce il lag, meglio va ... però comunque non basta alla causalità (probabilmente il campione tim è troppo poco significativo di tutti i dati telefonici per stabilire un legame causale ... ci sono molti operatori, e considerando che l'inglese è la lingua dominante è possibile che non abbiano operatori tim)

# COMMAND ----------

df2017 = spark.read.format("csv").option("header", "true").option("multiline",'true').option("inferSchema", 'true').load("dbfs:/FileStore/tables/Query_2017.csv")
print(df2017.count())
df2017 = df2017.dropna()
df2017 = df2017.na.fill("")
#get Date instead of String for Timestamp
from pyspark.sql.functions import *
df2017_with_date = df2017.withColumn("timestamp",from_unixtime(unix_timestamp('timestamp', 'HH:mm - dd MM yyyy')).cast("date")) #puoi anche usare "timestamp" per conservare anche l'ora, ma per ora non ti serve
df2017_with_date.createOrReplaceTempView("df_with_date")
df2017 = spark.sql("select timestamp, count(*) as total from df_with_date group by timestamp order by timestamp asc")
df2017.count()

# COMMAND ----------

display(df2017)

# COMMAND ----------

import pandas as pd
pandas_df2017 = df2017.toPandas()
pandas_df2017["timestamp"] = pd.to_datetime(pandas_df2017["timestamp"], format = "%Y-%m-%d")
pandas_df2017.dtypes

# COMMAND ----------

pandas_df2017 = pandas_df2017.set_index('timestamp')
pandas_df2017

# COMMAND ----------

pandas_series2017= pandas_df2017["total"]
pandas_series2017

# COMMAND ----------

visualize_stationarity(pandas_series2017,"timestamp")


# COMMAND ----------

test_stationarity(pandas_series2017)
#h0 = has unit root (ricorda, radice su cerchio unitario rende instabile la serie)


# COMMAND ----------

import numpy as np
pandas_series2017 = pandas_series2017.astype(np.float64)
pandas_series2017.dtypes

# COMMAND ----------

decomposition_tweet2017 = seasonal_decompose(pandas_series2017.values, freq =7)


# COMMAND ----------

trend_tweet2017 = decomposition_tweet2017.trend
fig, axes = plt.subplots()
axes.plot(trend_tweet2017,color='red',label='trend')
axes.plot(pandas_series2017.values, color = "blue", label="original")
plt.legend(loc='best')
display(fig)

# COMMAND ----------

seasonal_tweet2017 = decomposition_tweet2017.seasonal
fig, axes = plt.subplots()
axes.plot(seasonal_tweet2017,color='red',label='seasonal')
axes.plot(pandas_series2017.values, color = "blue", label="original")
plt.legend(loc='best')
display(fig)

# COMMAND ----------

#ATTENZIONE: residual != i residui della regressione, bensì è ciò che rimane dopo aver rimosso la non stazionarietà!!!!
residual_tweet2017 = decomposition_tweet2017.resid
fig, axes = plt.subplots()
axes.plot(residual_tweet2017,color='red',label='residual')
axes.plot(pandas_series2017.values, color = "blue", label="original")
plt.legend(loc='best')
display(fig)

# COMMAND ----------

res_tweet2017 = from_list_to_time_indexed_visualization(residual_tweet2017, '2016-12-31', '2017-12-30')

# COMMAND ----------

results_ARIMAX.params

# COMMAND ----------

import numpy
def predict(coef,history):
  yhat = 0.0
  for i in range(1,len(coef)+1):
    yhat += coef[i-1]*history[-i]
  return yhat

# COMMAND ----------

coef = results_ARIMAX.params
history = [res_tweet[i] for i in range(len(res_tweet))]
residuals_pred = [residual_ for residual_ in results_ARIMAX.resid.values]
predictions = list()
ytempar = list()
for t in range(len(res_tweet2017)):
  model = ARIMA(history, order = (1,0,1))
  model_fit = model.fit(trend="nc",disp=False)
  ar_coef,ma_coef = model_fit.arparams, model_fit.maparams
  resid = model_fit.resid
  yhat = predict(ar_coef,history)+predict(ma_coef,resid)
  predictions.append(yhat)
  obs = res_tweet2017[t]
  history.append(obs)

# COMMAND ----------

from sklearn.metrics import mean_squared_error

error = mean_squared_error(res_tweet2017, predictions)
print('Test MSE: %.3f' % error)

# COMMAND ----------

# plot
fig, axes = plt.subplots()
axes.plot(res_tweet2017.values)
axes.plot(predictions, color='red')
display(fig)
#come si può vedere, ha bisogno un attimo di avviarsi per inisiare ad andare a regime (ma è normale ... anche a regime i risultati non sono proprio favolosi ...)

# COMMAND ----------

print('RSS: %.4f'% np.sum((predictions-res_tweet2017)**2)) #abbiamo RSS molto alto, però è normale, un modello adattivo farebbe meglio

# COMMAND ----------

from pyspark.sql.types import LongType, StringType, StructField, StructType, BooleanType, ArrayType, IntegerType
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
castelnuovo = park_data.filter("name = 107")
castelnuovo.createOrReplaceTempView("castelnuovo")


# COMMAND ----------

clean_park = spark.sql("select name, avg(free) as avgfree, weekday, month, day, hour from castelnuovo where (month >=6 and month <= 10) and (year = 2016)  group by name, weekday, month, day, hour order by name, month,day,hour")
display(clean_park)

# COMMAND ----------

castelnuovo_clean= clean_park.select("name","avgfree","weekday", concat(lit("2016-"),col("month"),lit("-"),col("day"),lit("-"),col("hour")).alias("date"))

# COMMAND ----------

visual = clean_park.select("name","avgfree","weekday", concat(lit("2016-"),col("month"),lit("-"),col("day")).alias("date").cast(DateType()))
display(visual)

# COMMAND ----------

def get_pandas_series(df):
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
  
cast = get_pandas_series(castelnuovo_clean)
visualize(cast)

# COMMAND ----------

avgfree = cast.get("avgfree")


# COMMAND ----------

#from statsmodels.tsa.seasonal import seasonal_decompose
decomposition_cast = seasonal_decompose(avgfree.values, freq =24)

trend_cast = decomposition_cast.trend
fig, axes = plt.subplots(figsize=(20,5))
axes.plot(trend_cast,color='red',label='trend')
axes.plot(avgfree.values, color = "blue", label="original")
plt.legend(loc='best')
display(fig)

# COMMAND ----------

seasonal_cast = decomposition_cast.seasonal
fig, axes = plt.subplots(figsize=(20,5))
axes.plot(seasonal_cast,color='red',label='trend')
axes.plot(avgfree.values, color = "blue", label="original")
plt.legend(loc='best')
display(fig)

# COMMAND ----------

#ATTENZIONE: residual != i residui della regressione, bensì è ciò che rimane dopo aver rimosso la non stazionarietà!!!!
residual_cast = decomposition_cast.resid
fig, axes = plt.subplots(figsize=(20,5))
axes.plot(residual_cast,color='red',label='trend')
axes.plot(avgfree.values, color = "blue", label="original")
plt.legend(loc='best')
display(fig)

# COMMAND ----------

my_list = residual_cast.tolist()
rng = pd.date_range('2016-06-01 00','2016-10-31 22', freq='H')
res = pd.Series(my_list, index = rng)
res = res.dropna()
res.index.name = "timestamp"
test_stationarity(res)


# COMMAND ----------

chooseARIMAXOrder(res)
#p = da PACF = 3
#q =  = da ACTF = 6

# COMMAND ----------

model = ARIMA(res, order=(3, 0,6))  
results_ARIMA = model.fit(disp = -1)
pyDF_timeseries = fromPandasDFToDF(fromPandasSeriesToPandasDF(res), "timestamp")
pyDF_results_ARIMA = fromPandasDFToDF(fromPandasSeriesToPandasDF(results_ARIMA.fittedvalues), "timestamp")
pyDF_timeseries = pyDF_timeseries.withColumn("type",lit("count"))
pyDF_results_ARIMA = pyDF_results_ARIMA.withColumn("type",lit("ARIMA model"))
pyDF_all = pyDF_timeseries.union(pyDF_results_ARIMA)
display(pyDF_all)

# COMMAND ----------

print(results_ARIMA.summary())

# COMMAND ----------

from pandas import DataFrame
# plot residual errors
residuals = DataFrame(results_ARIMA.resid)
fig, axes = plt.subplots()
axes.plot(results_ARIMA.resid,color='red',label='residual')
fig.autofmt_xdate()

display(fig)


# COMMAND ----------

from scipy.stats import shapiro
stat, p = shapiro(residuals)
print(stat,p)
# interpret: gaussian residuals, ok!

# COMMAND ----------

print(residuals.describe())

# COMMAND ----------

def get_df_to_vis(data, start,end):
  my_list = data.tolist()
  rng = pd.date_range(start,end, freq='H')
  res = pd.Series(my_list, index = rng)
  res = res.dropna()
  res.index.name = "timestamp"
  visualize_stationarity(res, "timestamp")
  return res

# COMMAND ----------

trend_df_cast = get_df_to_vis(trend_cast, '2016-06-01 00', '2016-10-31 22') #l'effettivo(scartati i dropna) sarà da 2016-06-01 12 a 2016-10-31 10

# COMMAND ----------

seasonal_df_cast = get_df_to_vis(seasonal_cast, '2016-06-01 00', '2016-10-31 22')

# COMMAND ----------

avg_free_df_cast = get_df_to_vis(avgfree, '2016-06-01 00', '2016-10-31 22')

# COMMAND ----------

rebuilt = results_ARIMA.fittedvalues.add(trend_df_cast).add(seasonal_df_cast)
rebuilt

# COMMAND ----------

#riaggiungendo le non stazionarietà, com si può vedere le prestazioni sono molto buone!
pyDF_timeseries = fromPandasDFToDF(fromPandasSeriesToPandasDF(avg_free_df_cast), "timestamp")
pyDF_results_ARIMA = fromPandasDFToDF(fromPandasSeriesToPandasDF(rebuilt), "timestamp")
pyDF_timeseries = pyDF_timeseries.withColumn("type",lit("count"))
pyDF_results_ARIMA = pyDF_results_ARIMA.withColumn("type",lit("ARIMA model"))
pyDF_all = pyDF_timeseries.union(pyDF_results_ARIMA)
display(pyDF_all)

# COMMAND ----------

trend_cast = decomposition_cast.trend
fig, axes = plt.subplots(figsize=(20,5))
axes.plot(avg_free_df_cast,color='red',label='true')
axes.plot(rebuilt, color = "blue", label="predicted")
plt.legend(loc='best')
display(fig)

# COMMAND ----------

display(pyDF_all)

# COMMAND ----------

print('RSS: %.4f'% np.sum((avg_free_df_cast-rebuilt)**2))
#molto alto ... ma il segnale è fortemente nonstazionario e periodico ... delle variabili esogene potrebbero dare una mano, ma direi che può bastare

