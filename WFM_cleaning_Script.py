#!/usr/bin/env python
# coding: utf-8

# ## WFM Forecasting and Scheduling Assignment

# Loading associated packages (interfaces specialised in operational programming)

# In[1]:


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


# #### Reading file from path.

# In[ ]:


os.chdir('C:\\Users\\roy_shaw\\Desktop\\Work_Files\\Projects\\WFM Project\\inputs')


# Loading files to variables.

# In[ ]:


'''df1 = pd.read_excel('skillset 3-skillset 3 -April 1-30, 2020.xls')
df_1 = pd.read_excel('skillset 3-skillset 3 Apr 28-30-20.xls', header = 1)
df_Apr_20 = pd.concat([df1, df_1], ignore_index=True, axis = 0)
df2 = pd.read_excel('skillset 3-skillset 3 - May 1- 31, 2020.xls')
df_2 = pd.read_excel('skillset 3-skillset 3 May 28-31-20.xls', header = 1)
df_May_20 = pd.concat([df2, df_2], ignore_index=True, axis = 0)
df3 = pd.read_excel('skillset 3-skillset 3 - June 1 - 30, 2020.xls')
df_3 = pd.read_excel('skillset 3-skillset 3 June 28-31-20.xls', header = 1)
df_Jun_20 = pd.concat([df3, df_3], ignore_index=True, axis = 0)
df4 = pd.read_excel('skillset 3-skillset 3 - July 1 - 31, 2020.xls')
df_4 = pd.read_excel('skillset 3-skillset 3 July 28-31-20.xls', header = 1)
df_Jul_20 = pd.concat([df4, df_4], ignore_index=True, axis = 0)
df5 = pd.read_excel('skillset 3-skillset 3 - August 1 - 31, 2020.xls')
df_5 = pd.read_excel('skillset 3-skillset 3 Aug 28-31-20.xls', header = 1)
df_Aug_20 = pd.concat([df5, df_5], ignore_index=True, axis = 0)
df6 = pd.read_excel('skillset 3-skillset 3 - Sept 1-30, 2020.xls')
df_6 = pd.read_excel('skillset 3-skillset 3 Sep 28-30-20.xls', header = 1)
df_Sep_20 = pd.concat([df6, df_6], ignore_index=True, axis = 0)
df7 = pd.read_excel('skillset 3-skillset 3 - Oct 1 -31, 2020.xls')
df_7 = pd.read_excel('skillset 3-skillset 3 Oct 28-31-20.xls', header = 1)
df_Oct_20 = pd.concat([df7, df_7], ignore_index=True, axis = 0)
df8 = pd.read_excel('skillset 3-skillset 3 - Nov 1-30, 2020.xls')
df_8 = pd.read_excel('skillset 3-skillset 3 Nov 28-30-20.xls', header = 1)
df_Nov_20 = pd.concat([df8, df_8], ignore_index=True, axis = 0)
df9 = pd.read_excel('skillset 3-skillset 3 - Dec 1 - 31, 2020.xls')
df_9 = pd.read_excel('skillset 3-skillset 3 Dec 28-31-20.xls', header = 1)
df_Dec_20 = pd.concat([df9, df_9], ignore_index=True, axis = 0)
df10 = pd.read_excel('skillset 3-skillset 3 - Jan 1-31,2021.xls')
df_10 = pd.read_excel('skillset 3-skillset 3 Jan 28-31-20.xls', header = 1)
df_Jan_20 = pd.concat([df10, df_10], ignore_index=True, axis = 0)
df11 = pd.read_excel('skillset 3-skillset 3 - Feb 1-28, 2021.xls')
df_11 = pd.read_excel('skillset 3-skillset 3 Feb 28-28-20.xls', header = 1)
df_Feb_20 = pd.concat([df11, df_11], ignore_index=True, axis = 0)
df12 = pd.read_excel('skillset 3-skillset 3 - March 1 - 31, 2021.xls')
df_12 = pd.read_excel('skillset 3-skillset 3 March 28-31-20.xls', header = 1)
df_Mar_20 = pd.concat([df12, df_12], ignore_index=True, axis = 0)'''


# In[ ]:


'''df_FY21 = pd.concat([df_Apr_20, df_May_20, df_Jun_20, df_Jul_20, df_Aug_20, df_Sep_20, df_Oct_20, df_Nov_20, df_Dec_20,                     df_Jan_20, df_Feb_20, df_Mar_20], axis = 0, ignore_index=True)'''


# In[ ]:


'''df1 = pd.read_excel('skillset 3-skillset 3 - April 1 -30, 2021.xls')
df_1 = pd.read_excel('skillset 3-skillset 3 April 28-30-21.xls', header = 1)
df_Apr_21 = pd.concat([df1, df_1], ignore_index=True, axis = 0)
df2 = pd.read_excel('skillset 3-skillset 3 - May 1 - 31, 2021.xls')
df_2 = pd.read_excel('skillset 3-skillset 3 May 28-31-21.xls', header = 1)
df_May_21 = pd.concat([df2, df_2], ignore_index=True, axis = 0)
df3 = pd.read_excel('skillset 3-skillset 3 - June 1-30, 2021.xls')
df_3 = pd.read_excel('skillset 3-skillset 3 June 28-31-21.xls', header = 1)
df_Jun_21 = pd.concat([df3, df_3], ignore_index=True, axis = 0)
df4 = pd.read_excel('skillset 3-skillset 3 July 1-31,2021.xls')
df_4 = pd.read_excel('skillset 3-skillset 3 July 28-31-21.xls', header = 1)
df_Jul_21 = pd.concat([df4, df_4], ignore_index=True, axis = 0)
df5 = pd.read_excel('skillset 3-skillset 3 August 1-31, 2021.xls')
df_5 = pd.read_excel('skillset 3-skillset 3 Aug 28-31-21.xls', header = 1)
df_Aug_21 = pd.concat([df5, df_5], ignore_index=True, axis = 0)
df6 = pd.read_excel('skillset 3-skillset 3 September 1-30,2021.xls')
df_6 = pd.read_excel('skillset 3-skillset 3 Sep 28-30-21.xls', header = 1)
df_Sep_21 = pd.concat([df6, df_6], ignore_index=True, axis = 0)
df7 = pd.read_excel('skillset 3-skillset 3 October 1-31,2021.xls')
df_7 = pd.read_excel('skillset 3-skillset 3 Oct 28-30-21.xls', header = 1)
df_Oct_21 = pd.concat([df7, df_7], ignore_index=True, axis = 0)
df8 = pd.read_excel('skillset 3-skillset 3 November 1-30,2021.xls')
df_8 = pd.read_excel('skillset 3-skillset 3 Nov 28-30-21.xls', header = 1)
df_Nov_21 = pd.concat([df8, df_8], ignore_index=True, axis = 0)
df9 = pd.read_excel('skillset 3-skillset 3December1-31,2021.xls')
df_9 = pd.read_excel('skillset 3-skillset 3 Dec 28-31-21.xls', header = 1)
df_Dec_21 = pd.concat([df9, df_9], ignore_index=True, axis = 0)
df10 = pd.read_excel('skillset 3-skillset 3 January 1-31,2022.xls')
df_10 = pd.read_excel('skillset 3-skillset 3 Jan 28-31-22.xls', header = 1)
df_Jan_21 = pd.concat([df10, df_10], ignore_index=True, axis = 0)
df11 = pd.read_excel('skillset 3-skillset 3 February 1-28,2022.xls')
df_11 = pd.read_excel('skillset 3-skillset 3 Feb 28-28-22.xls', header = 1)
df_Feb_21 = pd.concat([df11, df_11], ignore_index=True, axis = 0)
df12 = pd.read_excel('skillset 3-skillset 3 March 1-31, 2022.xls')
df_12 = pd.read_excel('skillset 3-skillset 3 March 28-31-22.xls', header = 1)
df_Mar_21 = pd.concat([df12, df_12], ignore_index=True, axis = 0)'''


# In[ ]:


'''df_FY22 = pd.concat([df_Apr_21, df_May_21, df_Jun_21, df_Jul_21, df_Aug_21, df_Sep_21, df_Oct_21, df_Nov_21, df_Dec_21,                     df_Jan_21, df_Feb_21, df_Mar_21], axis = 0, ignore_index=True)'''


# In[ ]:


'''df1 = pd.read_excel('skillset 3-skillset 3 April 1-30,2022.xls')
df_1 = pd.read_excel('skillset 3-skillset 3 April 28-30-22.xls', header = 1)
df_Apr_22 = pd.concat([df1, df_1], ignore_index=True, axis = 0)
df2 = pd.read_excel('skillset 3-skillset 3 May 1-31,2022.xls')
df_2 = pd.read_excel('skillset 3-skillset 3 May 28-31-22.xls', header = 1)
df_May_22 = pd.concat([df2, df_2], ignore_index=True, axis = 0)
df3 = pd.read_excel('skillset 3-skillset 3 June1-31,2022.xls')
df_3 = pd.read_excel('skillset 3-skillset 3 June 28-30-22.xls', header = 1)
df_Jun_22 = pd.concat([df3, df_3], ignore_index=True, axis = 0)
df4 = pd.read_excel('skillset 3-skillset 3 July1-31,2022.xls')
df_4 = pd.read_excel('skillset 3-skillset 3 July 28-31-22.xls', header = 1)
df_Jul_22 = pd.concat([df4, df_4], ignore_index=True, axis = 0)
df5 = pd.read_excel('skillset 3-skillset 3 August1-31, 2022.xls')
df_5 = pd.read_excel('skillset 3-skillset 3 Aug 28-31-22.xls', header = 1)
df_Aug_22 = pd.concat([df5, df_5], ignore_index=True, axis = 0)
df6 = pd.read_excel('skillset 3-skillset 3 September 1-30, 2022.xls')
df_6 = pd.read_excel('skillset 3-skillset 3 Sep 28-30-22.xls', header = 1)
df_Sep_22 = pd.concat([df6, df_6], ignore_index=True, axis = 0)
df7 = pd.read_excel('skillset 3-skillset 3 October 1-31,2022.xls')
df_7 = pd.read_excel('skillset 3-skillset 3 Oct 28-31-22.xls', header = 1)
df_Oct_22 = pd.concat([df7, df_7], ignore_index=True, axis = 0)
df8 = pd.read_csv('skillset 3-skillset 3November1-30,2022.csv')
df_8 = pd.read_excel('skillset 3-skillset 3 Nov 28-30-22.xls', header = 1)
df_Nov_22 = pd.concat([df8, df_8], ignore_index=True, axis = 0)
df9 = pd.read_excel('skillset 3-skillset 3Decmber1-31,2022.xls')
df_9 = pd.read_excel('skillset 3-skillset 3 Dec 28-31-22.xls', header = 1)
df_Dec_22 = pd.concat([df9, df_9], ignore_index=True, axis = 0)
df10 = pd.read_excel('skillset 3-skillset 3 January1-31,2023.xls')
df_10 = pd.read_excel('skillset 3-skillset 3 Jan 28-31-23.xls', header = 1)
df_Jan_22 = pd.concat([df10, df_10], ignore_index=True, axis = 0)
df11 = pd.read_excel('skillset 3-skillset 3February1-28,2023.xls')
df_11 = pd.read_excel('skillset 3-skillset 3 Feb 28-28-23.xls', header = 1)
df_Feb_22 = pd.concat([df11, df_11], ignore_index=True, axis = 0)
df12 = pd.read_excel('March_FY23.xls')
df_12 = pd.read_excel('skillset 3-skillset 3 March 28-31-23.xls', header = 1)
df_Mar_22 = pd.concat([df12, df_12], ignore_index=True, axis = 0)'''


# In[ ]:


'''df_FY23 = pd.concat([df_Apr_22, df_May_22, df_Jun_22, df_Jul_22, df_Aug_22, df_Sep_22, df_Oct_22, df_Nov_22, df_Dec_22,                     df_Jan_22, df_Feb_22, df_Mar_22], axis = 0, ignore_index=True)'''


# In[ ]:


'''df1_ = pd.read_excel('skillset 30 min interval report April 2023.xls')
df2_ = pd.read_excel('skillset 3-skillset 3 - April 28 - 30-23.xls', header = 1)
df3_ = pd.read_excel('skillset 3-skillset 3 - 2023-05-15T145511.540_to_May-13.xls')'''


# In[ ]:





# In[ ]:


df_agg_clean = pd.concat([df_FY21, df_FY22, df_FY23, df1_, df2_, df3_], ignore_index=True)


# In[ ]:


df_agg_clean.columns


# In[ ]:


df_agg_clean = df_agg_clean.drop(columns = ['Unnamed: 0', 'Unnamed: 1', 'Unnamed: 2', 'Unnamed: 3', 'Unnamed: 4',                                  'Unnamed: 5', 'Unnamed: 6', 'Unnamed: 7'])


# In[ ]:


#df_agg_clean.drop_duplicates(keep='first', inplace=True)


# In[ ]:


'''df_FY21 = pd.read_csv('Inbound_FY21.csv')
df_FY22 = pd.read_csv('Inbound_FY22.csv')
df_FY23 = pd.read_csv('Inbound_FY23.csv')'''


# Condensing variables.

# In[ ]:


df_agg_clean = df_agg_clean.dropna(how='any')


# In[ ]:


df_agg_clean.to_csv(r'C:\Users\roy_shaw\Desktop\FY21-FY24_to_May-13.csv')


# In[ ]:


lst_stamp = []

lst_pre = list(df_agg_clean['interval_start_time'])
lst_post = list(df_agg_clean['interval_end_time'])

def conv_to_str(lst):
    return [str(i) for i in lst]

lst_che = conv_to_str(lst_pre)
lst_gav = conv_to_str(lst_post)


def sheer_str(lst):
    lst_conv = []
    for i in range(len(lst)):
        lst_conv.append(dateutil.parser.parse(lst[i], dayfirst = True))
    return(lst_conv)
            
lst_input = sheer_str(lst_che)
lst_follow = sheer_str(lst_gav)

df_agg_clean['interval_start_time'] = lst_input
df_agg_clean['interval_end_time'] = lst_follow


# In[ ]:


get_ipython().run_cell_magic('time', '', "df_agg_clean['interval_start_time'] = pd.to_datetime(df_agg_clean['interval_start_time'])\ndf_agg_clean['interval_end_time'] = pd.to_datetime(df_agg_clean['interval_end_time'])\ndf_agg_clean['trans_start'] = df_agg_clean['interval_start_time'].dt.strftime('%Y-%m-%d %H:%M:%S')\ndf_agg_clean['trans_end'] = df_agg_clean['interval_end_time'].dt.strftime('%Y-%m-%d %H:%M:%S')\ndf_agg_clean['date'] = pd.to_datetime(df_agg_clean['interval_start_time']).dt.date\n\nlst_strt = df_agg_clean['interval_start_time'].dt.strftime('%Y-%m-%d %H:%M:%S').str[11:]\nlst_end = df_agg_clean['interval_end_time'].dt.strftime('%Y-%m-%d %H:%M:%S').str[11:]\n\ntrans_start = lst_strt.tolist()\ntrans_end = lst_end.tolist()\n\ndf_agg_clean['interval_start_time'] = trans_start\ndf_agg_clean['interval_end_time'] = trans_end")


# In[ ]:


def fill_missing_with_median(line_group):
    median_value = line_group['calls_presented'].median()
    line_group['csq_name'].fillna(median_value, inplace=True)
    return line_group


# In[ ]:


#df_agg_clean['trans_start'].drop_duplicates(keep='first', inplace=True)


# In[ ]:


#df_agg_clean = df_agg_clean.groupby('csq_name').apply(fill_missing_with_median)


# In[ ]:


filler_4 = df_agg_clean.copy(deep = True)


# In[ ]:


#filler_4.to_csv(r'C:\Users\roy_shaw\Desktop\WFM_input.csv')


# Checking columns for Dataframe.

# We observe some extraneous columns which need be removed for memory optimization of forecasting algorithm.

# Removing NAs, Null Values and NaN values from the rows of the dataframe.

# Viewing the data for structural understanding.

# Programming gymnastics to separate the date time information from the date.

# Formatting time values from "text" to a datetime varible for computation.

# Only format new data using the below code then add to the repository.

# Exporting data to a csv file.

# In[ ]:


#filler_5['date_time'] = filler_5['date'] + ' ' + filler_5['interval_start_time']


# lst_date_time = list(filler_5['date_time'])
# lst_str8 = []
# for i in range(len(lst_date_time)):
#     lst_str8.append(pd.to_datetime(lst_date_time[i][:-3], format = '%Y-%m-%d %H:%M'))
# filler_5['date_time'] = lst_str8   

# In[ ]:


#filler_5['Day of week'] = filler_5['date'].dt.day_name()


# In[ ]:


print(type(filler_4['date'][0]))


# In[ ]:


filler_4['Day of week'] = filler_4['date'].apply(lambda x: x.strftime('%A'))


# In[ ]:


#filler_5.to_csv(r'C:\\Users\\roy_shaw\\Desktop\\Work_Files\\Projects\\WFM Project\\outputs\\df_manage_experiment.csv', index = False, encoding = 'utf-8')


# In[ ]:


filler_4.to_csv(r'C:\\Users\\roy_shaw\\Desktop\\Work_Files\\Projects\\WFM Project\\outputs\\df_update_manage.csv', index = False, encoding = 'utf-8')


# In[ ]:


filler_4.columns


# In[ ]:


print(type(df_new['interval_start_time'][1]))


# ## Break

# Removing extraneous values not relating to opening hours of Contact Centre.

# In[ ]:


from datetime import datetime


# In[ ]:


'''working_hrs_start = ['08:30 AM', '09:00 AM', '09:30 AM', '10:00 AM', '10:30 AM', '11:00 AM', '11:30 AM', '12:00 PM', '12:30 PM', '13:00 PM', '13:30 PM', '14:00 PM', '14:30 PM', '15:00 PM', '15:30 PM', '16:00 PM', '16:30 PM', '17:00 PM', '17:30 PM']
df_new = filler_5[filler_5['interval_start_time'].isin(working_hrs_start)]'''


# # Ya So!!!

# In[2]:


os.chdir('C:\\Users\\roy_shaw\\Desktop\\Work_Files\\Projects\\WFM Project\\outputs')


# In[3]:


df_new = pd.read_csv('df_manage_experiment.csv')


# In[4]:


df_new['date_time'] = df_new['date'] + ' ' + df_new['interval_start_time']


# In[7]:


lst_date_time = list(df_new['date_time'])
lst_str8 = []
for i in range(len(lst_date_time)):
    lst_str8.append(pd.to_datetime(lst_date_time[i][:-3], format = '%d/%m/%Y %H:%M'))
df_new['date_time'] = lst_str8   


# In[8]:


df_new['Day of week'] = df_new['date_time'].dt.day_name()


# ## Aggregate Contact Centre Daily Forecast - Analysis Beta

# In[9]:


plt.style.use('fivethirtyeight')
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import math


# In[10]:


result = df_new.groupby('date_time')['calls_presented'].sum().reset_index()


# In[11]:


result = result.rename(columns = {'date_time':'ds', 'calls_presented':'y'})


# In[12]:


result_ = result.set_index('ds')


# In[13]:


color_pal = ["#F8766D", "#D39200", "#93AA00",
             "#00BA38", "#00C19F", "#00B9E3",
             "#619CFF", "#DB72FB"]
result_.plot(style='.', figsize=(15,5), color=color_pal[0], title='Call Arrivals per Half Hour')
plt.tick_params(
    axis='x', which='both', bottom=False, top=False, labelbottom=False)
plt.show()


# In[14]:


split_date = '2023-02-15 17:30'
result_train = result_.loc[result_.index <= split_date].copy()
result_test = result_.loc[result_.index > split_date].copy()


# In[15]:


result_test.rename(columns={'y': 'TEST SET'})     .join(result_train.rename(columns={'y': 'TRAINING SET'}),
          how='outer') \
    .plot(figsize=(15,5), title='Call Arrivals Split', style='.')
plt.tick_params(
    axis='x', which='both', bottom=False, top=False, labelbottom=False)
plt.show()


# In[16]:


result_train.reset_index()     .rename(columns={'TRAINING SET':'y'}).tail()


# ## Training data using fbprophet package

# In[17]:


model = Prophet(weekly_seasonality=True, daily_seasonality=True, interval_width=0.95)
model.add_country_holidays(country_name='JM')
model.fit(result_train.reset_index()     .rename(columns={'TRAINING SET':'y'}))


# #### Forcasting from training data

# In[18]:


result_test_fcst = model.predict(df=result_test.reset_index()                                    .rename(columns={'TEST SET':'y'}))


# In[19]:


f, ax = plt.subplots(1)
f.set_figheight(5)
f.set_figwidth(15)
fig = model.plot(result_test_fcst,
                 ax=ax)
plt.show()


# #### Comparing Forecast with actual data

# In[20]:


f, ax = plt.subplots(1)
f.set_figheight(5)
f.set_figwidth(15)
ax.scatter(result_test.index, result_test['y'], color='r')
fig = model.plot(result_test_fcst, ax=ax)


# #### Error Metrics

# In[21]:


MSE = mean_squared_error(y_true=result_test['y'],
                   y_pred=result_test_fcst['yhat'])


# In[22]:


RMSE = math.sqrt(MSE)
RMSE


# In[23]:


print(result_test['y'],result_test_fcst['yhat'])


# In[24]:


R_square = r2_score(y_true=result_test['y'],
                   y_pred=result_test_fcst['yhat'])


# In[25]:


print(R_square)


# In[26]:


def mape(y_test, pred):
    y_test, pred = np.array(y_test), np.array(pred)
    mape = np.mean(np.abs((y_test - pred) / y_test))
    return mape


# ### Forcasting future date values

# In[27]:


model = Prophet(weekly_seasonality=True, daily_seasonality=True, interval_width=0.95)
model.add_country_holidays(country_name='JM')
model.fit(result_.reset_index())


# In[28]:


future = model.make_future_dataframe(periods=1440, freq = '30 min')
forecast = model.predict(future)


# In[29]:


get_ipython().run_cell_magic('time', '', "lst_input = list(forecast['ds'])\ntrans_start = []\ntrans_end = []\ntrans_date = []\nfor i in range(len(lst_input)):\n    df_trans_start = pd.to_datetime(forecast['ds'])\n    trans_start.append(df_trans_start.iloc[i].strftime('%H:%M %p'))\nforecast['Time_period'] = trans_start")


# In[51]:


working_hrs_start = ['08:30 AM', '09:00 AM', '09:30 AM', '10:00 AM', '10:30 AM', '11:00 AM', '11:30 AM', '12:00 PM', '12:30 PM', '13:00 PM', '13:30 PM', '14:00 PM', '14:30 PM', '15:00 PM', '15:30 PM', '16:00 PM', '16:30 PM', '17:00 PM', '17:30 PM']
df_new = forecast[forecast['Time_period'].isin(working_hrs_start)]


# In[52]:


erLang = df_new[df_new['ds'] >= datetime.datetime(2023, 4, 13, 8, 30, 0)]


# In[53]:


erLang


# In[54]:


erLang['ds']


# In[55]:


erLang['yhat'][erLang['yhat'] <= 0] = 1


# In[56]:


erlC_forecast = list(erLang['yhat'])


# In[49]:


#erlC_forecast = pd.DataFrame(erlC_forecast)


# In[39]:


#erlC_forecast.to_csv(r'C:\\Users\\roy_shaw\\Desktop\\forecast_erlangC.csv')


# In[40]:


#erlC_forecast = pd.read_csv(r'C:\\Users\\roy_shaw\\Desktop\\forecast_erlangC.csv')


# erlang = ErlangC(transactions=erlC_forecast, asa=20/60, aht=3, interval=1, shrinkage=0.3)
# requirements = erlang.required_positions(service_level=0.85, max_occupancy=0.85)
# print(requirements)

# In[59]:


from pyworkforce.queuing import MultiErlangC

param_grid = {"transactions": erlC_forecast, "aht": [4], "interval": [15], "asa": [20 / 60], "shrinkage": [0.30]}
multi_erlang = MultiErlangC(param_grid=param_grid, n_jobs=-1)
required_positions_scenarios = {"service_level": [0.85], "max_occupancy": [0.80]}
positions_requirements = multi_erlang.required_positions(required_positions_scenarios)
print("positions_requirements: ", positions_requirements)


# In[60]:


df_WFM = pd.DataFrame.from_dict(positions_requirements)


# In[61]:


df_WFM['date_time'] = erLang['ds'].values


# In[62]:


df_WFM['forecast'] = erLang['yhat'].values


# In[63]:


df_WFM


# In[64]:


df_WFM.to_csv(r'C:\Users\roy_shaw\Desktop\WFM_erLang_to_May_11.csv')


# ## Residual Analysis

# In[ ]:


import statsmodels.api as sm
import matplotlib.pyplot as plt
import matplotlib.dates as mdates


# In[ ]:


os.chdir('C:\\Users\\roy_shaw\\Desktop\\Work_Files\\Projects\\WFM Project\\outputs')


# In[ ]:


df_actual = pd.read_csv('df_manage_experiment.csv')


# In[ ]:


df_forecast = pd.read_csv('WFM_erLang_to_May_11.csv')


# In[ ]:


#working_hrs_start = ['08:30:00', '09:00:00', '09:30:00', '10:00:00', '10:30:00', '11:00:00', '11:30:00', '12:00:00', '12:30:00', '13:00:00', '13:30:00', '14:00:00', '14:30:00', '15:00:00', '15:30:00', '16:00:00', '16:30:00', '17:00:00', '17:30:00']


# In[ ]:


#df_actual['valid'] = df_actual['interval_start_time'].isin(working_hrs_start)


# In[ ]:


#df_actual['valid'].value_counts()


# In[ ]:


#df_actual = df_actual[df_actual['valid'] == True]


# In[ ]:


#df_actual['valid'].unique()


# In[ ]:


start = datetime.datetime(2023,4,13).date()
end = datetime.datetime(2023,5,11).date()


# In[ ]:


print(type(start))


# In[ ]:


df_period = df_actual.groupby('date')['calls_presented'].sum().reset_index()


# In[ ]:


df_period.head()


# In[ ]:


df_period['date'] = pd.to_datetime(df_period['date'], format = '%d/%m/%Y').dt.date


# In[ ]:


df_period = df_period[(df_period['date'] >= start) & (df_period['date'] <= end)]


# In[ ]:


df_period_sorted = df_period.sort_values(by='date').reset_index()


# In[ ]:


df_period_sorted


# In[ ]:


df_forecast['Datetime'] = pd.to_datetime(df_forecast['Datetime'], format = '%d/%m/%Y %H:%M')


# In[ ]:


df_forecast['date'] = df_forecast['Datetime'].dt.date


# In[ ]:


#df_period['date'] = pd.to_datetime(df_period['date'])


# In[ ]:


df_forecast.columns


# In[ ]:


df_forecast = df_forecast.groupby('date')['Forecasted Calls'].sum().reset_index()


# In[ ]:


df_forecast['Day_of_Week'] = df_forecast['date'].apply(lambda x: x.strftime('%A'))


# In[ ]:


#df_forecast = df_forecast[df_forecast['Day_of_Week'] != 'Sunday']


# In[ ]:


df_period_sorted['Day_of_Week'] = df_period_sorted['date'].apply(lambda x: x.strftime('%A'))


# In[ ]:


#df_period_sorted = df_period_sorted[df_period_sorted['Day_of_Week'] != 'Sunday']


# In[ ]:


df_forecast_sorted = df_forecast.sort_values(by='date')


# In[ ]:


df_forecast_sorted


# In[ ]:


df_period_sorted['Forecast'] = df_forecast_sorted['Forecasted Calls']


# In[ ]:


df_period_sorted['Residual'] = df_forecast_sorted['Forecasted Calls'] - df_period_sorted['calls_presented'] 


# In[ ]:


df_period_sorted


# In[ ]:


df_period_sorted.columns


# In[ ]:


df_period_sorted = df_period_sorted[['date', 'Forecast', 'calls_presented', 'Residual', 'Day_of_Week']]


# In[ ]:


df_period_sorted


# In[ ]:


plt.scatter(df_period_sorted['date'], df_period_sorted['Residual'])
plt.title('Difference between Forecasted Calls and Actual Calls Received')
plt.xlabel('Date')
plt.ylabel('Difference (Residual)')
plt.xticks(rotation = 'vertical')
plt.show()


# In[ ]:


import seaborn as sns

sns.histplot(df_period_sorted['Residual'], kde=True)
plt.xlabel('Residuals')
plt.ylabel('Frequency')
plt.title('Residual Distribution')
plt.show()

sm.qqplot(df_period_sorted['Residual'], line='s')
plt.title('Normal Q-Q Plot')
plt.show()


# In[ ]:


plt.scatter(df_period_sorted['Forecast'], df_period_sorted['Residual'])
plt.xlabel('Predicted Values')
plt.ylabel('Residuals')
plt.title('Residuals vs. Predicted Values')
plt.show()


# In[ ]:


from statsmodels.stats.stattools import durbin_watson
durbin_watson_stat = durbin_watson(df_period_sorted['Residual'])
print(durbin_watson_stat)


# In[ ]:


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


# In[ ]:





# In[ ]:


print(len(df_period), len(df_forecast))


# In[ ]:


df_output.to_csv(r'C:\\Users\\roy_shaw\\Desktop\\df_output.csv', index = False, encoding = 'utf-8')


# In[ ]:


shift_1['calls_presented'].plot(figsize = (12, 5), legend = True)
forecast_s1.plot(legend = True)


# 

# ## CCOLL Operating Line Analysis and Forecast - Approach ARIMA (Sidelined-Failed)

# In[ ]:


df_CCOLL = df_new[filler_5['csq_name'] == 'CCOLL-CSQ']
df_CQUER = df_new[filler_5['csq_name'] == 'CQUER-CSQ']
df_DLVRY = df_new[filler_5['csq_name'] == 'DLVRY-CSQ']
df_OPTOR = df_new[filler_5['csq_name'] == 'OPTOR-CSQ']
df_RPAIR = df_new[filler_5['csq_name'] == 'RPAIR-CSQ']
df_SALES = df_new[filler_5['csq_name'] == 'SALES-CSQ']


# In[ ]:


df_CCOLL.reset_index(inplace=True)


# In[ ]:


df_COLL = df_CCOLL.drop('index', axis = 1, inplace = True)


# In[ ]:


df_CCOLL['index_num'] = df_CCOLL.index.to_list()


# In[ ]:


df_CCOLL['shift_number'] = (df_CCOLL['index_num']%19) + 1


# In[ ]:


grouped = df_CCOLL.groupby(['shift_number'])
shift_1 = grouped.get_group(1)
shift_2 = grouped.get_group(2)
shift_3 = grouped.get_group(3)
shift_4 = grouped.get_group(4)
shift_5 = grouped.get_group(5)
shift_6 = grouped.get_group(6)
shift_7 = grouped.get_group(7)
shift_8 = grouped.get_group(8)
shift_9 = grouped.get_group(9)
shift_10 = grouped.get_group(10)
shift_11 = grouped.get_group(11)
shift_12 = grouped.get_group(12)
shift_13 = grouped.get_group(13)
shift_14 = grouped.get_group(14)
shift_15 = grouped.get_group(15)
shift_16 = grouped.get_group(16)
shift_17 = grouped.get_group(17)
shift_18 = grouped.get_group(18)
shift_19 = grouped.get_group(19)


# In[ ]:


shift_1.set_index('date', inplace = True)
shift_2.set_index('date', inplace = True)
shift_3.set_index('date', inplace = True)
shift_4.set_index('date', inplace = True)


# In[ ]:


arr1 = shift_1['calls_presented']


# In[ ]:


stepwise_fit = auto_arima(arr1, start_p=1, start_q=1, m=19, start_P=0, seasonal=True, d=None, D=1, trace=True, error_action='ignore', suppress_warnings=True, stepwise=True)


# In[ ]:


stepwise_fit.summary()


# In[ ]:


from statsmodels.tsa.statespace.sarimax import SARIMAX

model = SARIMAX(shift_1['calls_presented'], order = (1, 0, 0), seasonal_order =(2, 1, 0, 19))
result = model.fit()
  
# Forecast for the next month
forecast_s1 = result.predict(start = len(shift_1), end = (len(shift_1)-1) + 380, typ = 'levels').rename('Forecast')
  
# Plot the forecast values
shift_1['calls_presented'].plot(figsize = (12, 5), legend = True)
forecast_s1.plot(legend = True)


# In[ ]:


result_['calls_presented'].plot(figsize=(12,5))


# In[ ]:


from statsmodels.tsa.stattools import adfuller
def ad_test(dataset):
    dftest = adfuller(dataset, autolag = 'AIC')
    print(dftest[0], dftest[1])


# In[ ]:


ad_test(result_['calls_presented'])


# In[ ]:


from pmdarima import auto_arima
import warnings
warnings.filterwarnings("ignore")


# In[ ]:


stepwise_fit = auto_arima(result_['calls_presented'], trace=True, max_p = 7, supress_warnings=True)


# In[ ]:


stepwise_fit.summary()


# In[ ]:


from statsmodels.tsa.arima.model import ARIMA


# In[ ]:


split_date = '2023-01-31 17:30'
train = result_.loc[result_.index <= split_date].copy()
test = result_.loc[result_.index > split_date].copy()


# In[ ]:


model = ARIMA(train['calls_presented'], order = (0,1,0))
model = model.fit()
model.summary()


# In[ ]:


start = len(train)
end = len(train)+len(test)-1
pred=model.predict(start=start, end=end, typ='levels')
pred.index=result_.index[start:end+1]
print(pred)


# In[ ]:


pred.plot(legend=True)
test['calls_presented'].plot(legend=True)

