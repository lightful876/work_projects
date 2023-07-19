#!/usr/bin/env python
# coding: utf-8

# # Ecommerce Report generation for Operational Reporting 

# Importing required files and loading working packages for report generation.

# In[1]:


import pandas as pd
from datetime import datetime
import numpy as np
import os
os.chdir('C:\\Users\\roy_shaw\\Desktop\\Work_Files\\E-Commerce Report Automated Scripts\\Raw_Data\\Inputs\\FY-24\\Weekly')
df_comp = pd.read_csv('week_14_FY24_comp.csv')
df_pur = pd.read_csv('week_14_FY24_pur.csv')


# Here we are closing in on the records which have not been marked as a completed delivery. We do so by converting the empty string to a NaN value. We then create a column based on a Boolean to extract the records which fit the Null value criteria.
# The valuesin the dataframe df_pur now contains only values for completed orders purchased within the pre-determined timeframe.

# In[2]:


df_pur['Completed At'].replace('','NaN')
df_pur['Null_Bool'] = df_pur['Completed At'].isnull()
df_pur_empty = df_pur[df_pur['Null_Bool'] == True]
df_pur.drop(df_pur['Null_Bool'].index[df_pur['Null_Bool'] == True], inplace = True)


# Converting date field columns to a datetime type in order to run datetime related functions.

# In[3]:


df_pur['Completed At'] = pd.to_datetime(df_pur['Completed At']).dt.date
df_pur['Purchase Date'] = pd.to_datetime(df_pur['Purchase Date']).dt.date


# The below code combines the remainder of the purchases which were completed as detailed by 'In [2]'   

# In[4]:


df_main = pd.concat([df_pur, df_comp])


# Removing duplicated values from the main dataframe, for correctness.

# In[5]:


df_main.drop_duplicates(subset = "ID", keep = 'first', inplace = True)


# Replacing the string value of the purchase point from the region which the information is derived (the data pull from the ecommerce platform) for conciseness. In both the main dataframe and the dataframe detailing incompleted deliveries for purchased orders In [7] and In [8].

# In[6]:


df_main['Purchase Point'].replace(to_replace={'Antigua Website    Antigua Store        Antigua Store View': 'Antigua', 'Barbados Website    Barbados Store        Barbados Store View': 'Barbados',             'Belize Website    Belize Store        Belize Store View': 'Belize', 'Curacao Omni Website    Curacao Omni Store        Curacao Omni Store View': 'Curacao',              'Dominica Website    Dominica Store        Dominica Store View': 'Dominica',              'Grenada Website    Grenada Store        Grenada Store View': 'Grenada','Guyana Website    Guyana Store        Guyana Store View': 'Guyana',             'Jamaica Website    Jamaica Store        Jamaica Store View': 'Jamaica',              'St. Kitts and Nevis Website    St. Kitts and Nevis Store        St. Kitts and Nevis Store View': 'St. Kitts and Nevis',             'St. Lucia Website    St. Lucia Store        St. Lucia Store View': 'St. Lucia',             'St. Vincent Website    St. Vincent Store        St. Vincent Store View': 'St. Vincent',             'Trinidad and Tobago Website    Trinidad and Tobago Store        Trinidad and Tobago Store View': 'Trinidad and Tobago'}, inplace = True)


# In[7]:


df_pur_empty['Purchase Point'].replace(to_replace={'Antigua Website    Antigua Store        Antigua Store View': 'Antigua', 'Barbados Website    Barbados Store        Barbados Store View': 'Barbados',             'Belize Website    Belize Store        Belize Store View': 'Belize', 'Curacao Omni Website    Curacao Omni Store        Curacao Omni Store View': 'Curacao',              'Dominica Website    Dominica Store        Dominica Store View': 'Dominica',              'Grenada Website    Grenada Store        Grenada Store View': 'Grenada','Guyana Website    Guyana Store        Guyana Store View': 'Guyana',             'Jamaica Website    Jamaica Store        Jamaica Store View': 'Jamaica',              'St. Kitts and Nevis Website    St. Kitts and Nevis Store        St. Kitts and Nevis Store View': 'St. Kitts and Nevis',             'St. Lucia Website    St. Lucia Store        St. Lucia Store View': 'St. Lucia',             'St. Vincent Website    St. Vincent Store        St. Vincent Store View': 'St. Vincent',             'Trinidad and Tobago Website    Trinidad and Tobago Store        Trinidad and Tobago Store View': 'Trinidad and Tobago'}, inplace = True)


# The removal of the fields which are associated with these categories are removed based on business practices.

# In[8]:


df_main.drop(df_main.index[df_main['Shipping Information'] == 'Courts Carrier - Home delivery (Ready Finance payments have no fee)'], inplace = True)
df_main.drop(df_main.index[df_main['Shipping Information'] == 'Courts Carrier - Home Delivery Ready Finance  payments have no fee'], inplace = True)


# In[9]:


df_pur_empty.drop(df_pur_empty.index[df_pur_empty['Shipping Information'] == 'Courts Carrier - Home delivery (Ready Finance payments have no fee)'], axis = 0, inplace = True)
df_pur_empty.drop(df_pur_empty.index[df_pur_empty['Shipping Information'] == 'Courts Carrier - Home Delivery Ready Finance  payments have no fee'], axis = 0, inplace = True)


# Repeat of to_datetime for certainty in code not throwing errors.

# In[10]:


df_main['Completed At'] = pd.to_datetime(df_main['Completed At']).dt.date


# In[11]:


df_main['Purchase Date'] = pd.to_datetime(df_main['Purchase Date']).dt.date


# In[12]:


df_pur_empty['Purchase Date'] = pd.to_datetime(df_pur_empty['Purchase Date']).dt.date


# This was a text scrubbing exercise as the initial string contained superfluous characters.

# In[13]:


str_ = []
for i in range(0,len(df_main["Shipping Information"])):
    str_.append(df_main["Shipping Information"].values[i])
    i += 1
zen_ = pd.Series(str_, name="Shipping Information")
ten_ten = zen_.str.contains("Store Pickup")
for i in range(0,len(df_main["Shipping Information"])):
    if ten_ten[i] == True:
        df_main["Shipping Information"].replace(df_main["Shipping Information"].values[i],"Store Pickup", inplace = True)
        i += 1
    else:
        i += 1


# In[14]:


str_ = []
for i in range(0,len(df_pur_empty["Shipping Information"])):
    str_.append(df_pur_empty["Shipping Information"].values[i])
    i += 1
zen_ = pd.Series(str_, name="Shipping Information")
ten_ten = zen_.str.contains("Store Pickup")
for i in range(0,len(df_pur_empty["Shipping Information"])):
    if ten_ten[i] == True:
        df_pur_empty["Shipping Information"].replace(df_pur_empty["Shipping Information"].values[i],"Store Pickup", inplace = True)
        i += 1
    else:
        i += 1


# These entries in the records were renamed for both conciseness and business best-practice. Also deleting extraneous attributes.

# In[15]:


df_main['Status'].replace(to_replace={'canceled':'Canceled', 'complete':'Complete', 'Cancel POD':'Canceled', 'cancel_pod':'Canceled', 'complete_pod':'Complete', 'closed': 'Closed', 'holded':'Holded', 'pending':'Pending', 'pending_pod':'Pending', 'Pending Payment':'Pending', 'Pending Payment':'Pending', 'Pending POD':'Pending', 'PendingRF':'Pending', 'processing':'Processing', 'processing_pending':'Processing', 'Processing POD':'Processing', 'refund':'Canceled', 'Refund':'Canceled'}, inplace = True)
df_main['Payment Method'].replace(to_replace={'paymentondelivery':'Pay on Delivery','fac_gateway':'Credit Card/Debit Card','payinstore':'Pay in Store','checkmo':'Ready Finance Application','free':'Gift Card','emma':'EMMA','courts_storecard':'Courts Storecard', 'summasolutions_facgateway':'Credit/Debit Card','cashondelivery':'Pay on Delivery','paypal_standard':'Paypal'}, inplace = True)


# In[16]:


df_main.drop(["Billing Address", "Shipping Address", "Shipping and Handling", "Bill-to Name", "Ship-to Name", "Customer Email", "Customer Name", "Total Refunded", "Allocated sources", "Pickup Location Code", "Braintree Transaction Source", "Eye Lens Product"], axis = 1, inplace = True)
df_main["Shipping Information"].replace(to_replace={"Courts Carrier - Home delivery":"Courts Carrier", "Courts Carrier - FREE Home Delivery":"Courts Carrier", "Courts Carrier - Express Delivery":"Courts Carrier", "Courts Carrier - Home Delivery":"Courts Carrier","Free Shipping - Free delivery":"Free Shipping","Free Shipping - Free Shipping":"Free Shipping","Shipping Option - Free Shipping":"Free Shipping","Shipping Method - Express Delivery":"Courts Carrier",'Courts Carrier - Express PickUp - Incmplete':"Courts Carrier","Shipping Method - Free Delivery":"Free Delivery","Free Shipping - Free":"Free Delivery","Shipping Option - Express Pickup":"Store Pickup","Courts Carrier - FREE Home Delivery (*delivery by Christmas NOT guaranteed)":"Courts Carrier","Courts Carrier - Free Delivery":"Courts Carrier","Flat Rate - Home Delivery":"Flat Rate","Shipping Method - Home Delivery":"Free Shipping","Courts Carrier - FREE Delivery":"Free Shipping"}, inplace = True)


# In[17]:


df_pur_empty['Status'].replace(to_replace={'canceled':'Canceled', 'complete':'Complete', 'Cancel POD':'Canceled', 'cancel_pod':'Canceled', 'complete_pod':'Complete', 'closed': 'Closed', 'holded':'Holded', 'pending':'Pending', 'pending_pod':'Pending', 'Pending Payment':'Pending', 'Pending Payment':'Pending', 'PendingRF':'Pending','PendingPOD':'Pending','processing':'Processing', 'processing_pending':'Processing', 'processing_pod':'Processing', 'ProcessingPOD':'Processing','refund':'Canceled'}, inplace = True)
df_pur_empty['Payment Method'].replace(to_replace={'paymentondelivery':'Pay on Delivery','fac_gateway':'Credit Card/Debit Card','payinstore':'Pay in Store','checkmo':'Ready Finance Application','free':'Gift Card','emma':'EMMA','courts_storecard':'Courts Storecard', 'summasolutions_facgateway':'Credit/Debit Card','cashondelivery':'Pay on Delivery','paypal_standard':'Paypal'}, inplace = True)


# In[18]:


df_pur_empty.drop(["Billing Address", "Shipping Address", "Shipping and Handling", "Bill-to Name", "Ship-to Name", "Customer Email", "Customer Name", "Total Refunded", "Allocated sources", "Pickup Location Code", "Braintree Transaction Source", "Eye Lens Product"], axis = 1, inplace = True)
df_pur_empty["Shipping Information"].replace(to_replace={"Courts Carrier - Home delivery":"Courts Carrier", "Courts Carrier - FREE Home Delivery":"Courts Carrier", "Courts Carrier - Express Delivery":"Courts Carrier", "Courts Carrier - Home Delivery":"Courts Carrier","Free Shipping - Free delivery":"Free Shipping","Free Shipping - Free Shipping":"Free Shipping","Shipping Option - Free Shipping":"Free Shipping","Shipping Method - Express Delivery":"Courts Carrier",'Courts Carrier - Express PickUp - Incmplete':"Courts Carrier","Shipping Method - Free Delivery":"Free Delivery","Free Shipping - Free":"Free Delivery","Shipping Option - Express Pickup":"Store Pickup","Courts Carrier - FREE Home Delivery (*delivery by Christmas NOT guaranteed)":"Courts Carrier","Courts Carrier - Free Delivery":"Courts Carrier","Flat Rate - Home Delivery":"Flat Rate","Shipping Method - Home Delivery":"Free Shipping","Courts Carrier - FREE Delivery":"Free Shipping"}, inplace = True)


# Conditioning the data by dropping all NaN values that were present in the main dataframe's "Completed At" column.

# In[19]:


df_main.dropna(axis = 0, how = 'any', subset = ['Completed At'], inplace = True)


# Renaming all occurances of NaN to "Flat Rate" based on how this information is translated by business best-practices.

# In[20]:


df_main["Shipping Information"].fillna("Flat Rate",inplace = True)


# In[21]:


df_pur_empty["Shipping Information"].fillna("Flat Rate",inplace = True)


# Here we have extracted the payment details of all attemptes E-commerce transactions on the website from a third-party platform. We analyze data from the start of the quarter to the current date based on standard business protocol being that no order should be undelivered after 90 days.

# In[25]:


os.chdir('C:\\Users\\roy_shaw\\Desktop\\Work_Files\\E-Commerce Report Automated Scripts\\Raw_Data\\FAC Draw downs\\Weekly\\FY24')
df_pymt_4 = pd.read_excel('week_14_FAC.xlsx','rpTransaction')

os.chdir('C:\\Users\\roy_shaw\\Desktop\\Work_Files\\E-Commerce Report Automated Scripts\\Raw_Data\\FAC Draw downs\\Monthly\\FY24')
df_pymt_2 = pd.read_excel('FAC_Apr_FY24.xlsx', 'rpTransaction')
df_pymt_3 = pd.read_excel('FAC_May_FY24.xlsx', 'rpTransaction')
df_pymt_1 = pd.read_excel('FAC_Jun_FY24.xlsx', 'rpTransaction')


# Here we aggregate all the payment detail files into one payment master dataframe.

# In[26]:


df_prep = pd.concat([df_pymt_1, df_pymt_2, df_pymt_3, df_pymt_4])


# Filtering the transactions which were approved by the payment gateway. Leaving the transactions which were declined or 
# otherwise.

# In[27]:


df_filtered = df_prep[df_prep['Response Description'] != 'Approved']


# Here we determine the magnitude of records in the ecommerce dataframes which were pulled in the report, but were cancelled based on a mapping of the unique 'ID' key being present in the dataframe specifying transactions which were not approved.

# In[28]:


df_pur_empty['ID'].isin(df_filtered['Order ID']).value_counts()


# In[29]:


df_main['ID'].isin(df_filtered['Order ID']).value_counts()


# Removing said entries for data accuracy.

# In[30]:


df_main['Corrected_Bool'] = df_main['ID'].isin(df_filtered['Order ID'])
df_main.drop(df_main.index[df_main['Corrected_Bool'] == True], axis = 0, inplace = True)


# In[31]:


df_pur_empty['Corrected_Bool'] = df_pur_empty['ID'].isin(df_filtered['Order ID'])
df_pur_empty.drop(df_pur_empty.index[df_pur_empty['Corrected_Bool'] == True], axis = 0, inplace = True)


# Calculating the turn-around time for delivery of the orders which have been vetted based on the previous line of code.

# In[32]:


comp_date = list(df_main['Completed At'])
pur_date = list(df_main['Purchase Date'])
new_comp_date = []
TAT = []
CurrentDate = pd.datetime.now().date()
for cd,prd in zip(comp_date, pur_date):
    delta = (cd-prd).days
    new_comp_date.append(cd)
    TAT.append(max(1,delta))  
df_main['Completed At'] = new_comp_date
df_main['TAT'] = TAT


# Combining the scrubbed dataframes into one dataframe.

# In[33]:


new_df1 = pd.concat([df_main,df_pur_empty])


# This block of code is based on a specification in the records for Jamaica that would exclude certain transaction based on business operation (funds submitted for higher purchase payments).

# ### Change to OOP Script

# #### Jamaica

# In[34]:


df_validation = new_df1[new_df1['Purchase Point'] == 'Jamaica']
lst_GT = list(df_validation['Grand Total (Purchased)'])
lst_ST = list(df_validation['Subtotal'])
lst_validity = []
for i in range(len(lst_GT)):
    if (lst_GT[i] == lst_ST[i]) & (lst_GT[i]%100 == 0):
        lst_validity.append('False')
    else:
        lst_validity.append('True')
df_validation['is_RFpymt'] = lst_validity


# In[35]:


new_df1.drop(new_df1.index[new_df1['Purchase Point'] == 'Jamaica'], axis = 0, inplace = True)


# In[36]:


count_RF = df_validation[df_validation['is_RFpymt'] == 'False']


# In[37]:


df_validation.drop(df_validation.index[df_validation['is_RFpymt'] == 'False'],axis = 0, inplace = True)


# In[38]:


new_df2 = pd.concat([new_df1,df_validation])


# #### Barbados

# In[39]:


df_validation = new_df2[new_df2['Purchase Point'] == 'Barbados']
lst_GT = list(df_validation['Grand Total (Purchased)'])
lst_ST = list(df_validation['Subtotal'])
lst_validity = []
for i in range(len(lst_GT)):
    if (lst_GT[i] == lst_ST[i]) & (lst_GT[i]%2 == 0) & (lst_GT[i] <= 80):
        lst_validity.append('False')
    elif (lst_GT[i] == lst_ST[i]) & (lst_GT[i]%5 == 0) & (lst_GT[i] > 80) & (lst_GT[i] <= 360):
        lst_validity.append('False')
    elif (lst_GT[i] == lst_ST[i]) & (lst_GT[i]%10 == 0) & (lst_GT[i] > 360) & (lst_GT[i] <= 500):
        lst_validity.append('False')
    else:
        lst_validity.append('True')
df_validation['is_RFpymt'] = lst_validity


# In[40]:


new_df2.drop(new_df2.index[new_df2['Purchase Point'] == 'Barbados'], axis = 0, inplace = True)


# In[41]:


count_RF = count_RF.append(df_validation[df_validation['is_RFpymt'] == 'False'])


# In[42]:


df_validation.drop(df_validation.index[df_validation['is_RFpymt'] == 'False'],axis = 0, inplace = True)


# In[43]:


new_df3 = pd.concat([new_df2,df_validation])


# #### Guyana

# In[44]:


df_validation = new_df3[new_df3['Purchase Point'] == 'Guyana']
lst_GT = list(df_validation['Grand Total (Purchased)'])
lst_ST = list(df_validation['Subtotal'])
lst_validity = []
for i in range(len(lst_GT)):
    if (lst_GT[i] == lst_ST[i]) & (lst_GT[i]%100 == 0):
        lst_validity.append('False')
    else:
        lst_validity.append('True')
df_validation['is_RFpymt'] = lst_validity


# In[45]:


new_df3.drop(new_df3.index[new_df3['Purchase Point'] == 'Guyana'], axis = 0, inplace = True)


# In[46]:


count_RF = count_RF.append(df_validation[df_validation['is_RFpymt'] == 'False'])


# In[48]:


df_validation.drop(df_validation.index[df_validation['is_RFpymt'] == 'False'],axis = 0, inplace = True)


# In[49]:


new_df4 = pd.concat([new_df3,df_validation])


# #### OECS

# In[50]:


df_validation = new_df4[(new_df4['Purchase Point'] == 'St. Lucia') | (new_df4['Purchase Point'] == 'St. Vincent') |                       (new_df4['Purchase Point'] == 'St. Kitts and Nevis') | (new_df4['Purchase Point'] == 'Antigua') |                       (new_df4['Purchase Point'] == 'Grenada') | (new_df4['Purchase Point'] == 'Dominica')]
lst_GT = list(df_validation['Grand Total (Purchased)'])
lst_ST = list(df_validation['Subtotal'])
lst_validity = []
for i in range(len(lst_GT)):
    if (lst_GT[i] == lst_ST[i]) & (lst_GT[i]%10 == 0) & (lst_GT[i] <= 3000):
        lst_validity.append('False')
    else:
        lst_validity.append('True')
df_validation['is_RFpymt'] = lst_validity


# In[51]:


new_df4.drop(new_df4.index[(new_df4['Purchase Point'] == 'St. Lucia')|(new_df4['Purchase Point'] == 'St. Vincent') |                          (new_df4['Purchase Point'] == 'St. Kitts and Nevis') | (new_df4['Purchase Point'] == 'Antigua') |                          (new_df4['Purchase Point'] == 'Grenada') | (new_df4['Purchase Point'] == 'Dominica')]             , axis = 0, inplace = True)


# In[52]:


count_RF = count_RF.append(df_validation[df_validation['is_RFpymt'] == 'False'])


# In[53]:


df_validation.drop(df_validation.index[df_validation['is_RFpymt'] == 'False'],axis = 0, inplace = True)


# In[54]:


new_df5 = pd.concat([new_df4,df_validation])


# #### Belize

# In[55]:


df_validation = new_df5[new_df5['Purchase Point'] == 'Belize']
lst_GT = list(df_validation['Grand Total (Purchased)'])
lst_ST = list(df_validation['Subtotal'])
lst_validity = []
for i in range(len(lst_GT)):
    if (lst_GT[i] == lst_ST[i]) & (lst_GT[i]%5 == 0) & (lst_GT[i] <= 750):
        lst_validity.append('False')
    else:
        lst_validity.append('True')
df_validation['is_RFpymt'] = lst_validity


# In[56]:


new_df5.drop(new_df5.index[new_df5['Purchase Point'] == 'Belize'], axis = 0, inplace = True)


# In[57]:


count_RF = count_RF.append(df_validation[df_validation['is_RFpymt'] == 'False'])


# In[58]:


df_validation.drop(df_validation.index[df_validation['is_RFpymt'] == 'False'],axis = 0, inplace = True)


# In[59]:


new_df6 = pd.concat([new_df5,df_validation])


# Exporting dataframe to an excel file for visualization in PowerBI.

# In[60]:


new_df6.to_csv(r'C:\\Users\\roy_shaw\\Desktop\\Work_Files\\E-Commerce Report Automated Scripts\\Coding output\\Alt Channel Inputs\\Weekly\\week_14_FY24_ecom_rev.csv', index = False, encoding = 'utf-8')


# In[61]:


count_RF.to_csv(r'C:\\Users\\roy_shaw\\Desktop\\Work_Files\\E-Commerce Report Automated Scripts\\Coding output\\Alt Channel Inputs\\Weekly\\RF_week_14_FY24_ecom.csv', index = False, encoding = 'utf-8')

