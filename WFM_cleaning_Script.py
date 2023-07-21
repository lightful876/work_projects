#!/usr/bin/env python
# coding: utf-8

# WFM Forecasting and Scheduling Project

# Loading associated packages

import pandas as pd
import os
import numpy as np
import datetime
import dateutil.parser
import html5lib
import dateutil.parser
from bs4 import BeautifulSoup
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.kernel_ridge import KernelRidge
from statsmodels.tsa.seasonal import seasonal_decompose
from pmdarima import auto_arima
import dask.array as da
from prophet import Prophet
from pyworkforce.queuing import ErlangC


# Reading file from path.

os.chdir('C:\\Users\\roy_shaw\\Desktop\\Work_Files\\Projects\\WFM Project\\inputs')

# Loading files to variables.
df_agg_clean = pd.concat([df_FY21, df_FY22, df_FY23, df1_, df2_, df3_], ignore_index=True)

# Checking the columns found in the dataframe
df_agg_clean.columns

# Removing extraneous columns
df_agg_clean = df_agg_clean.drop(columns = ['Unnamed: 0', 'Unnamed: 1', 'Unnamed: 2', 'Unnamed: 3', 'Unnamed: 4',                                  'Unnamed: 5', 'Unnamed: 6', 'Unnamed: 7'])

# Removing duplicated values if any
df_agg_clean.drop_duplicates(keep='first', inplace=True)

# Dropping null values which would represent rows that are descriptive from the raw data pull.
df_agg_clean = df_agg_clean.dropna(how='any')

# Exporting aggregated dataframe to a csv document
df_agg_clean.to_csv(r'C:\Users\roy_shaw\Desktop\FY21-FY24_to_May-13.csv')

# Converting column entries to DateTime object type
lst_stamp = []
lst_pre = list(df_agg_clean['interval_start_time'])
lst_post = list(df_agg_clean['interval_end_time'])

def conv_to_str(lst):
    return [str(i) for i in lst]

lst_che = conv_to_str(lst_pre)
lst_gav = conv_to_str(lst_post)

# Converting from mm/dd/yyyy format to dd/mm/yyy
def sheer_str(lst):
    lst_conv = []
    for i in range(len(lst)):
        lst_conv.append(dateutil.parser.parse(lst[i], dayfirst = True))
    return(lst_conv)
            
lst_input = sheer_str(lst_che)
lst_follow = sheer_str(lst_gav)

df_agg_clean['interval_start_time'] = lst_input
df_agg_clean['interval_end_time'] = lst_follow

# Collecting the time data from the datetime component of the dataframe

get_ipython().run_cell_magic('time', '', "df_agg_clean['interval_start_time'] = pd.to_datetime(df_agg_clean['interval_start_time'])
\ndf_agg_clean['interval_end_time'] = pd.to_datetime(df_agg_clean['interval_end_time'])
\ndf_agg_clean['trans_start'] = df_agg_clean['interval_start_time'].dt.strftime('%Y-%m-%d %H:%M:%S')
\ndf_agg_clean['trans_end'] = df_agg_clean['interval_end_time'].dt.strftime('%Y-%m-%d %H:%M:%S')
\ndf_agg_clean['date'] = pd.to_datetime(df_agg_clean['interval_start_time']).dt.date
\nlst_strt = df_agg_clean['interval_start_time'].dt.strftime('%Y-%m-%d %H:%M:%S').str[11:]
\nlst_end = df_agg_clean['interval_end_time'].dt.strftime('%Y-%m-%d %H:%M:%S').str[11:]
\ntrans_start = lst_strt.tolist()
\ntrans_end = lst_end.tolist()
\ndf_agg_clean['interval_start_time'] = trans_start
\ndf_agg_clean['interval_end_time'] = trans_end")

# Accounting for any missing values in the dataframe
def fill_missing_with_median(line_group):
    median_value = line_group['calls_presented'].median()
    line_group['csq_name'].fillna(median_value, inplace=True)
    return line_group

# Creating a copy of the dataframe to preserve integrity and to avoid instances of running on multiple occasions
filler_4 = df_agg_clean.copy(deep = True)
filler_4.to_csv(r'C:\Users\roy_shaw\Desktop\WFM_input.csv')

# Creating a new conforming field to our required syntax
filler_5['date_time'] = filler_5['date'] + ' ' + filler_5['interval_start_time']

# Converting the syntax of the new datetime attribute
lst_date_time = list(filler_5['date_time'])
lst_str8 = []
for i in range(len(lst_date_time)):
   lst_str8.append(pd.to_datetime(lst_date_time[i][:-3], format = '%Y-%m-%d %H:%M'))
filler_5['date_time'] = lst_str8   

# Adding the day of the week to the analysis
filler_5['Day of week'] = filler_5['date'].dt.day_name()

# Applying an abbreviated version of the days
filler_4['Day of week'] = filler_4['date'].apply(lambda x: x.strftime('%A'))

# Exporting to a csv file
filler_5.to_csv(r'C:\\Users\\roy_shaw\\Desktop\\Work_Files\\Projects\\WFM Project\\outputs\\df_manage_experiment.csv', index = False, encoding = 'utf-8')
filler_4.to_csv(r'C:\\Users\\roy_shaw\\Desktop\\Work_Files\\Projects\\WFM Project\\outputs\\df_update_manage.csv', index = False, encoding = 'utf-8')

# Removing extraneous values not relating to opening hours of Contact Centre.
working_hrs_start = ['08:30 AM', '09:00 AM', '09:30 AM', '10:00 AM', '10:30 AM', '11:00 AM', '11:30 AM', '12:00 PM', '12:30 PM', '13:00 PM', '13:30 PM', '14:00 PM', '14:30 PM', '15:00 PM', '15:30 PM', '16:00 PM', '16:30 PM', '17:00 PM', '17:30 PM']
df_new = filler_5[filler_5['interval_start_time'].isin(working_hrs_start)]

# Reading in the csv file
# Adjusting to meet requirement
df_new = pd.read_csv('df_manage_experiment.csv')
df_new['date_time'] = df_new['date'] + ' ' + df_new['interval_start_time']

# Repeating script for df_new
lst_date_time = list(df_new['date_time'])
lst_str8 = []
for i in range(len(lst_date_time)):
    lst_str8.append(pd.to_datetime(lst_date_time[i][:-3], format = '%d/%m/%Y %H:%M'))
df_new['date_time'] = lst_str8   

df_new['Day of week'] = df_new['date_time'].dt.day_name()


# Aggregate Contact Centre Daily Forecast - Analysis Beta

# Preparing the plot for the time series data
plt.style.use('fivethirtyeight')

#Importing required class and aggregating the values d=for the number of calls daily
import math
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
result = df_new.groupby('date_time')['calls_presented'].sum().reset_index()

#Renaming columns based on requirements for the FBProphet package
result = result.rename(columns = {'date_time':'ds', 'calls_presented':'y'})
result_ = result.set_index('ds')

# Preparing paramenters for the plot
color_pal = ["#F8766D", "#D39200", "#93AA00",
             "#00BA38", "#00C19F", "#00B9E3",
             "#619CFF", "#DB72FB"]
result_.plot(style='.', figsize=(15,5), color=color_pal[0], title='Call Arrivals per Half Hour')
plt.tick_params(
    axis='x', which='both', bottom=False, top=False, labelbottom=False)
plt.show()

# Assigning the test and training data to initiate the machine learning model
split_date = '2023-02-15 17:30'
result_train = result_.loc[result_.index <= split_date].copy()
result_test = result_.loc[result_.index > split_date].copy()

result_test.rename(columns={'y': 'TEST SET'})     .join(result_train.rename(columns={'y': 'TRAINING SET'}),
          how='outer') \
    .plot(figsize=(15,5), title='Call Arrivals Split', style='.')
plt.tick_params(
    axis='x', which='both', bottom=False, top=False, labelbottom=False)
plt.show()

result_train.reset_index().rename(columns={'TRAINING SET':'y'}).tail()


# Training data using fbprophet package
model = Prophet(weekly_seasonality=True, daily_seasonality=True, interval_width=0.95)
model.add_country_holidays(country_name='JM')
model.fit(result_train.reset_index()     .rename(columns={'TRAINING SET':'y'}))


# Forcasting from training data with the model

result_test_fcst = model.predict(df=result_test.reset_index().rename(columns={'TEST SET':'y'}))

Overlaving the plot for the model results compared to the test data
f, ax = plt.subplots(1)
f.set_figheight(5)
f.set_figwidth(15)
fig = model.plot(result_test_fcst,
                 ax=ax)
plt.show()


# Comparing Forecast with test data graphically

f, ax = plt.subplots(1)
f.set_figheight(5)
f.set_figwidth(15)
ax.scatter(result_test.index, result_test['y'], color='r')
fig = model.plot(result_test_fcst, ax=ax)


# Error Metrics for the model

MSE = mean_squared_error(y_true=result_test['y'],
                   y_pred=result_test_fcst['yhat'])

RMSE = math.sqrt(MSE)
RMSE

R_square = r2_score(y_true=result_test['y'],
                   y_pred=result_test_fcst['yhat'])

def mape(y_test, pred):
    y_test, pred = np.array(y_test), np.array(pred)
    mape = np.mean(np.abs((y_test - pred) / y_test))
    return mape


# Forcasting future date values
model = Prophet(weekly_seasonality=True, daily_seasonality=True, interval_width=0.95)
model.add_country_holidays(country_name='JM')
model.fit(result_.reset_index())

future = model.make_future_dataframe(periods=1440, freq = '30 min')
forecast = model.predict(future)

working_hrs_start = ['08:30 AM', '09:00 AM', '09:30 AM', '10:00 AM', '10:30 AM', '11:00 AM', '11:30 AM', '12:00 PM', '12:30 PM', '13:00 PM', '13:30 PM', '14:00 PM', '14:30 PM', '15:00 PM', '15:30 PM', '16:00 PM', '16:30 PM', '17:00 PM', '17:30 PM']
df_new = forecast[forecast['Time_period'].isin(working_hrs_start)]

# Creating the range of dates for which the forecast will be applicable
erLang = df_new[df_new['ds'] >= datetime.datetime(2023, 4, 13, 8, 30, 0)]

# Absolute requirement for the values in the erlang C calculation
# Preparing to use forecasts in the Erlang C schedule predictor.
erLang['yhat'][erLang['yhat'] <= 0] = 1
erlC_forecast = list(erLang['yhat'])

# Exporting dataframe to csv format
erlC_forecast = pd.read_csv(r'C:\\Users\\roy_shaw\\Desktop\\forecast_erlangC.csv')

#Applying the Erlang C model to the forecasts for which the schedule is required
from pyworkforce.queuing import MultiErlangC

param_grid = {"transactions": erlC_forecast, "aht": [4], "interval": [15], "asa": [20 / 60], "shrinkage": [0.30]}
multi_erlang = MultiErlangC(param_grid=param_grid, n_jobs=-1)
required_positions_scenarios = {"service_level": [0.85], "max_occupancy": [0.80]}
positions_requirements = multi_erlang.required_positions(required_positions_scenarios)
print("positions_requirements: ", positions_requirements)

Exporting the values in the resultant dictionary output to a dataframe
df_WFM = pd.DataFrame.from_dict(positions_requirements)
df_WFM['date_time'] = erLang['ds'].values

# Adding values for the forecasted calls to add more information
df_WFM['forecast'] = erLang['yhat'].values

# Exporting data to a csv file
df_WFM.to_csv(r'C:\Users\roy_shaw\Desktop\WFM_erLang_to_May_11.csv')


# Residual Analysis -  This section is about examining the model's performance based on the realization of the dates in the forecast

# Importing relevant classes
import statsmodels.api as sm
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# Reading in the file to a database
df_actual = pd.read_csv('df_manage_experiment.csv')
df_forecast = pd.read_csv('WFM_erLang_to_May_11.csv')

# Removing extraneous information from the forecasted times
working_hrs_start = ['08:30:00', '09:00:00', '09:30:00', '10:00:00', '10:30:00', '11:00:00', '11:30:00', '12:00:00', '12:30:00', '13:00:00', '13:30:00', '14:00:00', '14:30:00', '15:00:00', '15:30:00', '16:00:00', '16:30:00', '17:00:00', '17:30:00']
df_actual['valid'] = df_actual['interval_start_time'].isin(working_hrs_start)
df_actual = df_actual[df_actual['valid'] == True]

# Selecting range for the time series
start = datetime.datetime(2023,4,13).date()
end = datetime.datetime(2023,5,11).date()

# Testing based on the aggregated calls for the days required
df_period = df_actual.groupby('date')['calls_presented'].sum().reset_index()

# Converting column to datetime and applying the filtering of values criteria based on the date
df_forecast['Datetime'] = pd.to_datetime(df_forecast['Datetime'], format = '%d/%m/%Y %H:%M')
df_period['date'] = pd.to_datetime(df_period['date'], format = '%d/%m/%Y').dt.date
df_period = df_period[(df_period['date'] >= start) & (df_period['date'] <= end)]

#Sorting by date
df_period_sorted = df_period.sort_values(by='date').reset_index()

#Removing time components
df_forecast['date'] = df_forecast['Datetime'].dt.date
df_period['date'] = pd.to_datetime(df_period['date'])

#Aggregating the data forecasted
#Applying conditionalities for Day of the Week
df_forecast = df_forecast.groupby('date')['Forecasted Calls'].sum().reset_index()
df_forecast['Day_of_Week'] = df_forecast['date'].apply(lambda x: x.strftime('%A'))

#Removing the Sundays from the script
df_forecast = df_forecast[df_forecast['Day_of_Week'] != 'Sunday']

#Applying a specific condensed version of the Day of the Week in the dataframe
df_period_sorted['Day_of_Week'] = df_period_sorted['date'].apply(lambda x: x.strftime('%A'))
df_period_sorted = df_period_sorted[df_period_sorted['Day_of_Week'] != 'Sunday']

#Adding required calculations to begin the residual analysis
df_forecast_sorted = df_forecast.sort_values(by='date')
df_period_sorted['Forecast'] = df_forecast_sorted['Forecasted Calls']
df_period_sorted['Residual'] = df_forecast_sorted['Forecasted Calls'] - df_period_sorted['calls_presented'] 

df_period_sorted = df_period_sorted[['date', 'Forecast', 'calls_presented', 'Residual', 'Day_of_Week']]

# Residual vs Datetime function
plt.scatter(df_period_sorted['date'], df_period_sorted['Residual'])
plt.title('Difference between Forecasted Calls and Actual Calls Received')
plt.xlabel('Date')
plt.ylabel('Difference (Residual)')
plt.xticks(rotation = 'vertical')
plt.show()

#QQ Plot
import seaborn as sns

sns.histplot(df_period_sorted['Residual'], kde=True)
plt.xlabel('Residuals')
plt.ylabel('Frequency')
plt.title('Residual Distribution')
plt.show()

sm.qqplot(df_period_sorted['Residual'], line='s')
plt.title('Normal Q-Q Plot')
plt.show()

#Residuals vs Predicted values
plt.scatter(df_period_sorted['Forecast'], df_period_sorted['Residual'])
plt.xlabel('Predicted Values')
plt.ylabel('Residuals')
plt.title('Residuals vs. Predicted Values')
plt.show()

#Testing for Autocorrelation
from statsmodels.stats.stattools import durbin_watson
durbin_watson_stat = durbin_watson(df_period_sorted['Residual'])
print(durbin_watson_stat)

# Group the data by day of the week and calculate the average of the residuals
residual_avg = df_period_sorted.groupby('Day_of_Week')['Residual'].mean()

# Create a list of weekdays to use as x-axis labels
weekdays = ['Friday', 'Monday', 'Saturday', 'Sunday', 'Thursday', 'Tuesday', 'Wednesday']

# Plot the average residuals as a bar chart
plt.bar(weekdays, residual_avg)

# Customize the plot
plt.xlabel('Day of the Week')
plt.xticks(rotation='vertical')
plt.ylabel('Average Residual')
plt.title('Average Residual by Day of the Week')

# Show the plot
plt.show()

#Converting to CSV
df_output.to_csv(r'C:\\Users\\roy_shaw\\Desktop\\df_output.csv', index = False, encoding = 'utf-8')
