#!/usr/bin/env python
# coding: utf-8

# # Campaign Successes Analysis - OECS

# ## Loading Packages/Data and logging on to Database remotely.

# In[4]:


import pandas as pd
import os
import pyodbc
from itertools import product
import numpy as np
import seaborn as sns
from textwrap import wrap
import matplotlib.pyplot as plt
import scipy.stats as stats
import random
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from sklearn.preprocessing import OneHotEncoder
from sklearn.preprocessing import LabelEncoder
from sqlalchemy import create_engine
from scipy.stats import chi2_contingency
from scipy.stats import pearsonr, spearmanr
from pulp import LpVariable, LpProblem, lpSum, LpMaximize
'''from sqlalchemy.connectors import pyodbc
from sqlalchemy.engine.url import make_url'''
import urllib.parse
import openpyxl


# Connecting to the database

# In[52]:


server = '10.192.26.32'
database = 'CODS'
#driver = 'xxxxxx'
username = 'roy_shaw'
password = 'SpriteVvs#41605**'
cnxn = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};""Server=10.192.26.32;""Database=CODS;""trusted_connection=yes;""UID=roy_shaw;""PWD=SpriteVvs#41605*;")
cursor = cnxn.cursor()


# ### Reading in Campaign Conversions data from CODS Database. 

# Calling records from the database that captures purchases in aggregate for the total deliveries made in the reporting period

# #### Change Date for Below

# In[53]:


df_campaign_success = pd.read_sql("SELECT * FROM CODS.dbo.[Credit.BookingsAndDeliveriesMar2023]",cnxn)


# In[54]:


filler_1 = df_campaign_success


# In[55]:


filler_1.rename(columns = {'Customer ID':'MasterIDNumber','Account Number':'MostRecentAccount'}, inplace = True)


# Matching Primary Keys to standard (As Customer Code) main file 'Campaign Blast' denoted df_jun here has Country Customer Master as the unique Identifier

# Calling records from the database that captures all transactions completed in the last 3 years with a filter on the customer's latest account being later than the start of the reporting period. Here we are exporing accounts that were open after December 2022.

# #### Change Below for adjustment to the date analyzed.

# In[56]:


df_Control = pd.read_sql("SELECT * FROM CODS.dbo.[RPT_CL_HP_CUSTS_ACCT_SMRY] WHERE DateAcctOpen > '2023-03-01'",cnxn)


# Aliasing the dataframe to avoid querying the database on every instance of erroneous data being retrieved.

# In[57]:


filler_2 = df_Control


# Adjusting the names of attributes in the dataframe for standardization.

# In[58]:


filler_2.rename(columns = {'Custid':'MasterIDNumber', 'Account_Number':'Acctno'}, inplace = True)


# Calling records from the database that captures all transactions that represent customers who took the campaign offer.

# #### Change Below for adjustment to the date analyzed

# Importing a query for the credit campaign successes had in the month of December.

# In[59]:


df_Master_Purchases = pd.read_sql("SELECT [CountryCustomerID], [MasterIDNumber], [Account Number] ,[CampaignCode], [Agreement Total], [Country], [Account Type], [Customer ID], [Date Account Opened], [Delivered Disbursed Date], [Delivery Status], [ISO], [Terms Type] FROM CODS.dbo.[vwCampaignSuccessMar2023]".format('db'),cnxn)


# Aliasing the results of the query

# In[60]:


filler_3 = df_Master_Purchases


# Adjusting the attributes from the query

# In[61]:


filler_3.rename(columns = {'Account Number':'Acctno'}, inplace = True)


# In[62]:


filler_3.drop(['Delivered Disbursed Date'], axis = 1, inplace = True)


# Compiling Text Blast Information to one dataframe, we will remove the duplication from multiple textblasts sent to one individual.

# Database query as the list of customers who were distributed campaign blast for December.

# #### Change Date Here

# In[63]:


df_main = pd.read_sql("SELECT * FROM [CODS].[dbo].[Credit.CampaignListMar2023]",cnxn)


# Aliasing the query for process efficiencies

# In[64]:


filler_4 = df_main


# We merge the dataframes for the credit campaigns issued via textblast (filler 4) to the deliveries that were made for customers in the reporting period (filler 1) on the notion that the textblast was accepted and a delivery was made in that reporting period.

# In[65]:


filler_4 = filler_4.merge(filler_1, on = 'MasterIDNumber', how = 'inner')


# We use a function to check the occurrence of the unique identification key (in this case MasterIDNumber) on the list of (accepted) textblasts in the dataframe for the customer information from the 3 year customer master (represented by a True/False assignment). This is done so that the attributes which were not available in the accepted textblast (due to the normalization of criteria of the database).

# In[66]:


filler_4['Take-up Bool'] = filler_4['MasterIDNumber'].isin(filler_3['MasterIDNumber'])


# Removing extraneous columns.

# Pre-FY23

# In[67]:


'''filler_4.drop(['Firstname', 'LastName', 'RFCreditLimit', 'AvailableSpend', 'HomeTel', 'WorkTel',              'MobileTel','OutstandingBalance', 'AgreementTotal', 'USDAvailableSpend',               'MostRecentAccount_x', 'Agreement Total', 'Service Charge', 'Principle Amount',              'Delivery Status', 'Country_y', 'ISO_y'], axis = 1, inplace = True)'''


# In[68]:


filler_4.columns


# Post-FY23

# In[69]:


filler_4.drop(['firstname', 'name', 'RFCreditLimit', 'AvailableSpend', 'Arrears', 'HomeTel', 'WorkTel',              'MobileTel','OutstandingBalance', 'AgreementTotal', 'USDAvailableSpend', 'ArrearsUSD', 'DateSettled',              'MostRecentAccount_x', 'Delivered Disbursed Date', 'Agreement Total', 'Service Charge', 'Principle Amount',              'Terms Type', 'Account Type', 'Delivery Status', 'Country_y', 'ISO_y'], axis = 1, inplace = True)


# Drawing down the scope of the analysis to the region/country of interest (in this case the OECS) by creating a boolean variable to confirm the record is of interest to the analysis which we will filter using the same boolean value.

# In[70]:


filler_4['Clean_Order'] = filler_4['ISO_x'].isin(['AG','DM','GD','KN','LC','VC'])
filler_4.drop(filler_4['Clean_Order'].index[filler_4['Clean_Order'] == False], axis = 0, inplace = True)


# Creating a new dataframe filtering for the truth value for the customer data being matched to the customer master.

# In[71]:


df_tkup_success = filler_4[filler_4['Take-up Bool'] == True]


# In[72]:


df_tkup_success = df_tkup_success.rename(columns = {'MostRecentAccount_y':'Acctno'})


# In[73]:


df_tkup_fail = filler_4[filler_4['Take-up Bool'] == False]


# Now we merge the filtered dataframe to the customer master dataframe to access certain attributes not available in the original table. This is an outer join so the tables will combine and there will be Na values ijntroduced to the dataframe.

# In[74]:


df_prod_camp_key = df_tkup_success.merge(filler_2, on = 'Acctno', how = 'outer')


# Removing the Na values from our primary key values.

# In[75]:


df_prod_camp_key = df_prod_camp_key.dropna(subset = ['CountryCustomerID'])
df_prod_camp_key = df_prod_camp_key.dropna(subset = ['MasterIDNumber_y'])


# Taking the information from the most recent account, from the credit campaign successes.

# In[76]:


df_prod_camp_key = df_prod_camp_key.merge(filler_3, on = 'Acctno', how = 'inner')


# Removing extraneous columns.

# In[77]:


df_prod_camp_key.drop(['Account Type','MasterIDNumber_y', 'MasterIDNumber_x', 'Agreement Total',                       'FirstName','LastName','RFCreditLimit','AvailableSpend','OutstandingBalance',                       'Arrears','No_of_CLN','No_of_HP','CLN_Agrtotal','HP_Agrtotal', 'ServiceCharge',                       'TotalInstalments', 'ISO_x', 'WorstStatusCode','MthlyIncome','Birthdate','Gender','Age_y',                       'Occupation_y','CountryCustomerID_y','Agreement Total', 'MostRecentAcctno',                       'Country_x','Date Account Opened_y','Take-up Bool','Clean_Order',                       'Date Account Opened_x', 'Is_Cashloan_Acct','Is_HPorRF_Acct','Is_Both','Item','ItemType',                       'ItemCatCode', 'ServiceCharge', 'DivisionCode', 'DepartmentCode', 'ItemNo2', 'ClassCode', 'ClassID',                       'ClassName', 'RFCreditLimit_USD', 'AvailableSpend_USD',                       'OutstandingBalance_USD', 'Arrears_USD', 'CLN_Agrtotal_USD', 'HP_Agrtotal_USD',                       'TotalInstalments_USD', 'MthlyIncome_USD', 'ServiceCharge_USD', 'DatasetLastUpdated', 'HomeTel',                       'MobileTel', 'WorkTel', 'Email', 'CustAddr1', 'CustAddr2', 'CusPOCode', 'Country_y'],                      axis = 1, inplace = True)


# We use the information from the account numbers to determine the location of the store that the campaign was taken up (as the first 3 digits of the account number).

# In[78]:


'''lst_Rcnt_Ac = df_prod_camp_key['MostRecentAccount']
lst_Brnch_Num = []
for i in range(len(lst_Rcnt_Ac)):
    lst_Brnch_Num.append(lst_Rcnt_Ac[i][:3])
df_prod_camp_key['StoreID'] = lst_Brnch_Num'''


# Reordering the columns to our specifications.

# In[79]:


df_prod_camp_key.columns


# In[80]:


df_prod_camp_key = df_prod_camp_key[['CountryCustomerID_x','MasterIDNumber','ISO_y', 'DivisionName', 'DepartmentName',                                     'RfavailablePercent','Acctno', 'Is_Cancelled', 'DateAccOpenMth', 'DateAcctOpen',                                     'AgreementTotal_USD','CampaignCode_x','CampaignCode_y', 'DeliveryFlag', 'DeliveryDate',                                     'Itemno', 'ItemCategory','Terms Type', 'BranchName','dateborn',                                     'Age_x','sex','maritalstat','Occupation_x']]


# Removing the duplicated values for our primary key and other attributes from the system's repeat of records.

# In[81]:


df_prod_camp_key.drop_duplicates(subset = ['CountryCustomerID_x','CampaignCode_x','CampaignCode_y'], keep =                                           'first', inplace = True)


# Loading in base files

# Matching the store ID obtained from 'trimming' the account number to that from the base file which has the name for the corresponding store.

# In[82]:


'''os.chdir('C:\\Users\\roy_shaw\\Desktop\\Work_Files\\OECS-Campaign Results')
df_store = pd.read_excel('Store_Finder-OECS.xlsx', sheet_name = 'Store_Finder-OECS')
lst_StoreID = list(df_store['StoreID'])
new_StoreID = []
df_store.drop('StoreID', axis = 1, inplace = True)
for i in range(len(lst_StoreID)):
    new_StoreID.append(str(lst_StoreID[i]))
df_store['StoreID'] = new_StoreID
df_prod_camp_key = df_prod_camp_key.merge(df_store, how = 'inner', on = 'StoreID')'''


# Special exeption to the code for this product had to be considered based on how the business reports successes for campaigns for this product.

# In[83]:


df_SFI = df_prod_camp_key[df_prod_camp_key['CampaignCode_x'] == 'SFI']
df_prod_camp_key.drop(df_prod_camp_key.index[df_prod_camp_key['CampaignCode_x'] == 'SFI'], inplace = True)


# In[84]:


lst_CampCode_off = list(df_prod_camp_key['CampaignCode_x'])
lst_CampCode_upt = list(df_prod_camp_key['CampaignCode_y'])
lst_Harmony = []
for i in range(len(lst_CampCode_off)):
    if lst_CampCode_off[i] != lst_CampCode_upt[i]:
        lst_Harmony.append(False)
    else:
        lst_Harmony.append(True)
df_prod_camp_key['Harmony'] = lst_Harmony


# In[85]:


df_prod_camp_key.drop(df_prod_camp_key.index[df_prod_camp_key['Harmony'] == False], inplace = True)
df_prod_camp_key.drop('Harmony', axis = 1, inplace = True)


# In[86]:


df_prod_camp_key = pd.concat([df_prod_camp_key, df_SFI])


# In[87]:


df_prod_camp_key.drop_duplicates(subset = ['CountryCustomerID_x','CampaignCode_x','CampaignCode_y'], keep =                                           'first', inplace = True)


# In[88]:


filler_conv = df_main[(df_main['ISO'] == 'DM') | (df_main['ISO'] == 'GD') | (df_main['ISO'] == 'AG') |                      (df_main['ISO'] == 'KN') | (df_main['ISO'] == 'LC') | (df_main['ISO'] == 'VC')]


# In[89]:


filler_conv['Conversion'] = filler_conv['CountryCustomerID'].isin(df_prod_camp_key['CountryCustomerID_x'])


# ### Data Cleaning and Manipulation

# In[90]:


filler_conv['Conversion'].value_counts()


# #### Change Date Here

# In[91]:


import datetime
date_ = datetime.date(2023, 3, 1)


# #### Important! resurface this input if required

# In[92]:


'''cust_life = []
trans_date = list(pd.to_datetime(df_prod_camp_key['EarliestDateAcctOpen']).apply(lambda x: x.date()))
end_date_cur = [date.today()]*len(trans_date)
for trn,end in zip(trans_date, end_date_cur):
    delta = (end-trn).days/365.25
    cust_life.append(delta)
df_prod_camp_key['Customer Lifetime'] = cust_life'''

#df_prod_camp_key['Customer Lifetime']


# ### Data Preparation

# Checking the fields and determining the most reliable and scientific method to produce the optimal result.

# In[93]:


df_prod_camp_key['Occupation_x'].replace(to_replace = {"Business Owner".upper():"Managers","Civil Servant".upper():"Professional","Comm Agents".upper():"Technicians & Associate Professionals","Comm Agents":"Technicians & Associate Professionals","Domestic/Janitor".upper():"Elementary Occupations","Driver".upper():"Plant/Machine Operators/Assemblers","Entertainment".upper():"Professional","Factory Workers".upper():"Elementary Occupations",
                                                       "Farming".upper():"Skilled Agriculture/Forestry/Fishery Workers","Fisherman".upper():"Skilled Agriculture/Forestry/Fishery Workers","Forces".upper():"Armed Forces","Forces":"Armed Forces","Hotel Staff".upper():"Service and Sales Workers","Managerial".upper():"Managers","Manual/Labourer".upper():"Elementary Occupations","Minibus Driver".upper():"Plant/Machine Operators/Assemblers",
                                                       "Nurse".upper():"Technicians & Associate Professionals","Police".upper():"Armed Forces","Professional".upper():"Professional","Restuarant/Bar".upper():"Service and Sales Workers","Retail".upper():"Service and Sales Workers","Security".upper():"Elementary Occupations","Skilled Trade".upper():"Technicians & Associate Professionals","Taxi Driver".upper():"Plant/Machine Operators/Assemblers", 
                                                       "Teacher".upper():"Professional","Vendor".upper():"Service and Sales Workers","Waiting/Bar".upper():"Service and Sales Workers","CASHIER/TELLER":"Clerical Support Workers", "Restrnt / Bar":"Service and Sales Workers", "CLERICAL":"Clerical Support Workers","Self Employed":"Managers", "Porter/Handyman":"Elementary Occupations", "Labourer/Agriculture":"Skilled Agriculture/Forestry/Fishery Workers",
                                                      "OTHER":"Other", "Tradesman":"Craft and Related Trades Workers", "HUCKSTER":"Service and Sales Workers", "POLICE/FIREFIGHTER":"Service and Sales Workers", "PENSIONER":"Pensioner", "JOURNALIST":"Professional", "ORDERLY":"Technicians & Associate Professionals", "CLERGY":"Professional","TOUR GUIDE": "Service and Sales Workers","BAILIFF":"Service and Sales Workers",np.nan:"BLANK",
                                                      "INSURANCE AGENT":"Technicians & Associate Professionals","SHOP KEEPER":"Service and Sales Workers", "DOMESTIC":"Elementary Occupations", "CARPENTER":"Craft and Related Trades Workers", "JANITOR":"Elementary Occupations","RESTAURANT/BAR":"Service and Sales Workers","FACTORY WORKER":"Elementary Occupations","SUPERVISOR":"Technicians & Associate Professionals",
                                                        "TECHNICIAN":"Technicians & Associate Professionals","CHILD CARER":"Service and Sales Workers", "CHEF":"Technicians & Associate Professionals", "CASHIER":"Clerical Support Workers","MACHINE OPERATOR":"Plant/Machine Operators/Assemblers","HOUSE KEEPING":"Service and Sales Workers","LANDSCAPING":"Professional","MASON":"Craft and Related Trades Workers", "STEVEDOR":"Elementary Occupations",
                                                        "STEVEDORE":"Elementary Occupations","HAIRDRESSER":"Service and Sales Workers","TECHNICAL OPERATOR":"Plant/Machine Operators/Assemblers","OFFICE ATTENDANT":"Clerical Support Workers","SALES CLERK/ASSISTANT":"Clerical Support Workers", "MAINTENANCE WORKER":"Elementary Occupations","Athletic Coach":"Technicians & Associate Professionals","ELECTRICIAN":"Technicians & Associate Professionals",
                                                        "ACCOUNTANT":"Professional","RECEPTIONIST":"Clerical Support Workers","ADMINISTRATIVE CLERK":"Clerical Support Workers", "ENGINEER":"Professional", "SOCIAL WORKER":"Professional","STORE ROOM CLERK":"Clerical Support Workers","RAMP ATTENDANT":"Elementary Occupations","ACCOUNTS CLERK":"Clerical Support Workers","SECRETARY":"Clerical Support Workers","BAKER AT BAKERY":"Service and Sales Workers",
                                                        "PRISON OFFICER": "Services and Sales Workers", "BUS CONDUCTOR":"Elementary Occupations", "COMM AGENT":"Technicians & Associate Professionals","TELE-CALLERS":"Service and Sales Workers","Mechanic":"Craft and Related Trades Workers","MECHANIC":"Craft and Related Trades Workers","CATERER":"Technicians & Associate Professionals", "PHARMACIST":"Professional", "MEDICAL DOCTOR":"Professional","CONSULTANT":"Professional","PLUMBER":"Craft and Related Trades Workers",
                                                        "NAIL TECHNICIAN":"Service and Sales Workers","TRUCK CONDUCTOR":"Elementary Occupations","SEAMSTRESS":"Craft and Related Trades Workers","DIVER-COMMERCIAL":"Plant/Machine Operators/Assemblers","BABY SITTING":"Service and Sales Workers","SURVEYOR":"Professional","SALES SUPERVISOR":"Service and Sales Workers","RADIO ANNOUNCER":"Professional","RADIO ANNOUCER":"Professional","PLANT OPERATOR":"Plant/Machine Operators/Assemblers",
                                                        "ROOF INSTALLER":"Craft and Related Trades Workers", "OPERATIONS":"Professional", "REAL ESTATE OWNER":"Other","LINES MAN":"Craft and Related Trades Workers","FINANCIAL CONTROLLER":"Professional","MAIL CARRIER":"Clerical Support Workers","GALLEY STEWARD":"Elementary Occupations", "FOREMAN":"Plant/Machine Operators/Assemblers", "REAL ESTATE AGENT":"Professional","VETENARIAN":"Professional","PHOTOGRAPHER":"Service and Sales Workers","JDF":"Armed Forces",
                                                      "FLIGHT ATTENDANT":"Services and Sales Workers", "Retail":"Service and Sales Workers","Hotel Staff":"Service and Sales Workers"}, inplace=True)


# In[94]:


from datetime import datetime
start_gen_Z = datetime.strptime('01/1/1997','%d/%m/%Y').date()
end_gen_Z = datetime.strptime('31/12/2012','%d/%m/%Y').date()
start_mil = datetime.strptime('01/1/1981','%d/%m/%Y').date()
end_mil = datetime.strptime('31/12/1996','%d/%m/%Y').date()
start_gen_X = datetime.strptime('01/1/1965','%d/%m/%Y').date()
end_gen_X = datetime.strptime('31/12/1980','%d/%m/%Y').date()
start_boom_2 = datetime.strptime('01/1/1955','%d/%m/%Y').date()
end_boom_2 = datetime.strptime('31/12/1964','%d/%m/%Y').date()
start_boom_1 = datetime.strptime('01/1/1946','%d/%m/%Y').date()
end_boom_1 = datetime.strptime('31/12/1954','%d/%m/%Y').date()
start_post = datetime.strptime('01/1/1928','%d/%m/%Y').date()
end_post = datetime.strptime('31/12/1945','%d/%m/%Y').date()


# In[95]:


lst = []
lst_pre = list(filler_conv['dateborn'])
for i in range(len(lst_pre)):
    if ((pd.to_datetime(lst_pre[i], dayfirst = True) >= start_gen_Z) & (pd.to_datetime(lst_pre[i], dayfirst = True) <= end_gen_Z)):
        lst.append('Gen Z')
        i+=1
    elif ((pd.to_datetime(lst_pre[i], dayfirst = True) >= start_mil) & (pd.to_datetime(lst_pre[i], dayfirst = True) <= end_mil)):
        lst.append('Millenial')
        i+=1
    elif ((pd.to_datetime(lst_pre[i], dayfirst = True) >= start_gen_X) & (pd.to_datetime(lst_pre[i], dayfirst = True) <= end_gen_X)):
        lst.append('Gen X')
        i+=1
    elif ((pd.to_datetime(lst_pre[i], dayfirst = True) >= start_boom_2) & (pd.to_datetime(lst_pre[i], dayfirst = True) <= end_boom_2)):
        lst.append('Boomers 2')
        i+=1
    elif ((pd.to_datetime(lst_pre[i], dayfirst = True) >= start_boom_1) & (pd.to_datetime(lst_pre[i], dayfirst = True) <= end_boom_1)):
        lst.append('Boomers 1')
        i+=1
    elif ((pd.to_datetime(lst_pre[i], dayfirst = True) >= start_post) & (pd.to_datetime(lst_pre[i], dayfirst = True) <= end_post)):
        lst.append('Post War')
        i+=1
    else:
        lst.append('WW2')
        i+=1
        
filler_conv['Age Group'] = lst


# In[96]:


lst = []
lst_pre = list(df_prod_camp_key['dateborn'])
for i in range(len(lst_pre)):
    if ((pd.to_datetime(lst_pre[i], dayfirst = True) >= start_gen_Z) & (pd.to_datetime(lst_pre[i], dayfirst = True) <= end_gen_Z)):
        lst.append('Gen Z')
        i+=1
    elif ((pd.to_datetime(lst_pre[i], dayfirst = True) >= start_mil) & (pd.to_datetime(lst_pre[i], dayfirst = True) <= end_mil)):
        lst.append('Millenial')
        i+=1
    elif ((pd.to_datetime(lst_pre[i], dayfirst = True) >= start_gen_X) & (pd.to_datetime(lst_pre[i], dayfirst = True) <= end_gen_X)):
        lst.append('Gen X')
        i+=1
    elif ((pd.to_datetime(lst_pre[i], dayfirst = True) >= start_boom_2) & (pd.to_datetime(lst_pre[i], dayfirst = True) <= end_boom_2)):
        lst.append('Boomers 2')
        i+=1
    elif ((pd.to_datetime(lst_pre[i], dayfirst = True) >= start_boom_1) & (pd.to_datetime(lst_pre[i], dayfirst = True) <= end_boom_1)):
        lst.append('Boomers 1')
        i+=1
    elif ((pd.to_datetime(lst_pre[i], dayfirst = True) >= start_post) & (pd.to_datetime(lst_pre[i], dayfirst = True) <= end_post)):
        lst.append('Post War')
        i+=1
    else:
        lst.append('WW2')
        i+=1
        
df_prod_camp_key['Age Group'] = lst


# In[97]:


from tabulate import tabulate
table = [['Generation Label', 'Age-Group'], ['Gen Z','18-25'],['Millenial','26-41'],['Gen X','42-57'],['Boomers 2','58-67'],['Boomers 1','68-76'],['Post War','77-94'],['WW2','>95']]
print(tabulate(table, headers = "firstrow"))


# In[98]:


df_prod_camp_key.columns


# #### Change Date Here

# In[99]:


df_prod_camp_key['YearMonth'] = ['202303']*len(df_prod_camp_key['CountryCustomerID_x'])
filler_conv['YearMonth'] = ['202303']*len(filler_conv['CountryCustomerID'])


# #### Change name of output file for adjustment to date analyzed

# In[100]:


with pd.ExcelWriter(r'C:\Users\roy_shaw\Desktop\output_Mar_FY23.xlsx') as writer:
    df_prod_camp_key.to_excel(writer, sheet_name = 'Main')
    filler_conv.to_excel(writer, sheet_name = 'Aggregate')


# Aggregating the monthly outputs

# In[134]:


lst_file_name = []
df_agg_main = pd.DataFrame()
df_agg_supp = pd.DataFrame()
directory = r"C:\Users\roy_shaw\Desktop\Work_Files\Campaign Analysis\Cleaned Data-OECS\FY23\output_data"
for filename in os.scandir(f'{directory}'):
    if filename.is_file():
        lst_file_name.append(filename.path[-20:-5])
lst_set_file_name = list(set(lst_file_name))
for i in range(len(lst_set_file_name)):
    curr_df = pd.read_excel(f'{directory}\{lst_set_file_name[i]}.xlsx', sheet_name = 'Main')
    post_df = pd.read_excel(f'{directory}\{lst_set_file_name[i]}.xlsx', sheet_name = 'Aggregate')
    df_agg_main = pd.concat([df_agg_main, curr_df])
    df_agg_supp = pd.concat([df_agg_supp, post_df])
    curr_df = pd.DataFrame()
    post_df = pd.DataFrame()

df_front = df_agg_main.to_csv(r'C:\\Users\\roy_shaw\\Desktop\\Work_Files\\Campaign Analysis\\Cleaned Data-OECS\\FY23\\df_main_agg.csv', index = False, encoding = 'utf-8')
df_escrt = df_agg_supp.to_csv(r'C:\\Users\\roy_shaw\\Desktop\\Work_Files\\Campaign Analysis\\Cleaned Data-OECS\\FY23\\df_agg_totl.csv', index = False, encoding = 'utf-8')           


# In[152]:


os.chdir('C:\\Users\\roy_shaw\\Desktop\\Work_Files\\Campaign Analysis\\Cleaned Data-OECS\\FY23')
df_agg_main = pd.read_csv('df_main_agg.csv')


# In[153]:


df_agg_main = df_agg_main[df_agg_main['Is_Cancelled'] == 'N']


# In[154]:


exclude_items = ['DT', 'ADMIN', 'LOAN', 'PPP', 'SD', 'RASTP1', 'RASTB3']
df_agg_main_HP = df_agg_main[~df_agg_main['Itemno'].isin(exclude_items)]


# In[155]:


df_agg_main_HP['DepartmentName'].value_counts().to_csv(r'C:\Users\roy_shaw\Desktop\categories_ttl.csv')


# In[ ]:


from matplotlib import rcParams
rcParams['figure.figsize'] = 12, 9


# In[ ]:


Monthly_Margins_Count = 12


# In[ ]:


'''df_GZ = df_agg_main[df_agg_main['Age Group'] == 'Gen Z']
df_ML = df_agg_main[df_agg_main['Age Group'] == 'Millenial']
df_GX = df_agg_main[df_agg_main['Age Group'] == 'Gen X']
dfBM2 = df_agg_main[df_agg_main['Age Group'] == 'Boomers 2']
dfBM1 = df_agg_main[df_agg_main['Age Group'] == 'Boomers 1']
df_PW = df_agg_main[df_agg_main['Age Group'] == 'Post War']'''


# ## Approach Alpha - HP

# In[156]:


df_dummy_input = df_agg_main_HP[['Age Group', 'DepartmentName']]
df_dummy_input_1 = df_agg_main_HP[['BranchName', 'DepartmentName']]


# In[157]:


df_encoded = pd.get_dummies(df_dummy_input)
df_encoded_1 = pd.get_dummies(df_dummy_input_1)


# In[158]:


def cramers_v(x, y):
    confusion_matrix = pd.crosstab(x, y)
    chi2 = chi2_contingency(confusion_matrix)[0]
    n = confusion_matrix.sum().sum()
    phi2 = chi2 / n
    r, k = confusion_matrix.shape
    phi2corr = max(0, phi2 - ((k-1)*(r-1))/(n-1))
    rcorr = r - ((r-1)**2)/(n-1)
    kcorr = k - ((k-1)**2)/(n-1)
    v = np.sqrt(phi2corr / min((kcorr-1), (rcorr-1)))
    return v


# #### Segmentation by Age

# In[159]:


rows= []

for var1 in df_encoded:
    col = []
    for var2 in df_encoded :
        cramers =cramers_v(df_encoded[var1], df_encoded[var2]) # Cramer's V test
        col.append(round(cramers,2)) # Keeping of the rounded value of the Cramer's V  
    rows.append(col)
  
    cramers_results = np.array(rows)
df = pd.DataFrame(cramers_results, columns = df_encoded.columns, index =df_encoded.columns)


# #### Segmentation by Location

# In[162]:


rows= []

for var1 in df_encoded_1:
    col = []
    for var2 in df_encoded_1:
        cramers =cramers_v(df_encoded_1[var1], df_encoded_1[var2]) # Cramer's V test
        col.append(round(cramers,2)) # Keeping of the rounded value of the Cramer's V  
    rows.append(col)
  
    cramers_results = np.array(rows)
df_1 = pd.DataFrame(cramers_results, columns = df_encoded_1.columns, index =df_encoded_1.columns)


# In[164]:


df.to_csv(r'C:\Users\roy_shaw\Desktop\Work_Files\Campaign Analysis\Cleaned Data-OECS\FY23\corr_correct_OECS.csv')


# In[165]:


df_1.to_csv(r'C:\Users\roy_shaw\Desktop\Work_Files\Campaign Analysis\Cleaned Data-OECS\FY23\corr_correct_loc_OECS.csv')


# In[172]:


# Step 1: Calculate total sales for each age group and retail category
total_sales_age = df_agg_main_HP.groupby(['Age Group'])['AgreementTotal_USD'].sum()
total_sales_category = df_agg_main_HP.groupby(['DepartmentName'])['AgreementTotal_USD'].sum()

# Step 2: Calculate percentage of total sales for each age group and category
pct_sales_age = total_sales_age / total_sales_age.sum()
pct_sales_category = total_sales_category / total_sales_category.sum()


# Step 3: Calculate correlation between sales for each category and age group
df_ = pd.read_csv(r'C:\Users\roy_shaw\Desktop\Work_Files\Campaign Analysis\Cleaned Data-OECS\FY23\corr_correct_OECS.csv', index_col = 0)

lst_cat = ['ACCESSORIES AND OTHERS', 'APPLIANCES FURNITURE','ASHLEY BEDDING', 'ASHLEY DINING',           'ASHLEY ENTERTAINMENT','ASHLEY MASTER BEDROO', 'ASHLEY MOTION', 'ASHLEY OCCASIONAL', 'ASHLEY STATIONARY',           'AUDIO', 'AUDIO RSK', 'BABY GOODS','BEDDING', 'BEDROOM', 'BICYCLES', 'BLANK MEDIA', 'CAMPING','CAR AUDIO',           'CELL PHONES','CELLPHONES AND ACCESSORIES', 'COMPUTER', 'COMPUTER AND ACCESSORIES ', 'COOLING AND HEATING',           'DECORATIVE ACCESSORIES ', 'DIGITAL STORAGE', 'DINING', 'ELECTRICAL GENERATORS', 'FRIDGE',           'FURNITURE OCCASSIONAL MISC', 'GAMES AND TOYS', 'GAMING', 'GAMING ACCESSORIES', 'GAMING ACCESSORIES',           'GAMING CONSOLES', 'GAMING PCS', 'HARDWARE', 'HEADPHONES EARPHONES', 'HEALTH', 'HOME AND OFFICE', 'HOUSEWARES',           'INDIVIDUAL SPORT', 'KITCHEN', 'LAPTOPS', 'LENSES', 'LOUGE', 'MUSICAL INSTRUMENTS', 'OFFICE',           'OFFICE SYSTEMS', 'OPTICAL ACCESSORIES', 'OTHERS RSK PRODUCTS', 'OUTDOOR', 'OUTDOOR SPORT', 'PERSONAL CARE',           'PREMIUM FRAMES', 'PRINTERS', 'REGULAR FRAMES', 'REPO ELECTRICAL', 'REPO FURNITURE', 'SEWING MACHINES'           'SMALL APPLIANCES', 'SPARE PARTS FOR ELECTRICAL', 'STORAGE AND ORGANIZATION', 'STOVES', 'SUNGLASSES',           'TABLETS', 'TRANSPORT', 'TV AND ACCESSORIES', 'VEHICLES ACCESSORIES', 'VENTILATION', 'VIDEO GAMES',           'VISION', 'WASHING MACHINE AND DRYER']
lst_age = list(df_.columns)
lst_corr = []
lst_obj_i = []
lst_obj_j = []
for i in range(len(lst_cat)):
    lst_obj_i += [lst_cat[i]]*71
    for j in range(len(lst_age)):
        lst_obj_j.append(lst_age[j])
lst_corr = df_.values.tolist()

data = {'category':[i for i in lst_obj_i],
       'age_group':[i for i in lst_obj_j],
       'corr':[x for l in lst_corr for x in l]}

df_new = pd.DataFrame(data)

df_new.set_index(['category', 'age_group'], inplace=True)

# convert the index to a list of tuples and then to a dictionary
correlations = df_new.index.tolist()
correlations = {key: value for key, value in zip(correlations, df_new['corr'])}

# Step 4: Define decision variables
categories = df_agg_main_HP['DepartmentName'].unique()
age_groups = df_agg_main_HP['Age Group'].unique()
allocation = LpVariable.dicts("allocation", [(category, age_group) for category in categories for age_group in age_groups], lowBound=0, cat='Continuous')

# Step 5: Set up linear programming model
model = LpProblem("Retail Category Allocation", LpMaximize)

# Objective function
model += lpSum([correlations[(category, age_group)] * allocation[(category, age_group)] * total_sales_age[age_group] for age_group in age_groups for category in categories if category and (category, age_group) in correlations])

# Constraints
for age_group in age_groups:
    model += lpSum([allocation[(category, age_group)] for category in categories]) == pct_sales_age[age_group] * total_sales_age.sum()
for category in categories:
    model += lpSum([allocation[(category, age_group)] for age_group in age_groups]) >= 1e-6

# Step 6: Solve the optimization problem
model.solve()

# Print the optimal allocation of retail categories to age groups
for age_group in age_groups:
    print(f"Age Group {age_group}:")
    for category in categories:
        print(f"    {category}: {allocation[(category, age_group)].varValue}")
        
allocation_df = pd.DataFrame(allocation_data)

# Reshape the dataframe
allocation_pivot = allocation        
allocation_df = pd.DataFrame(allocation_data)


# In[173]:


allocation_df.to_csv(r'C:\Users\roy_shaw\Desktop\allocation_age_OECS.csv')


# In[168]:


# Step 1: Calculate total sales for each location and retail category
total_sales_post = df_agg_main_HP.groupby(['PostCode'])['AgreementTotal_USD'].sum()
total_sales_category = df_agg_main_HP.groupby(['ItemCategory'])['AgreementTotal_USD'].sum()

# Step 2: Calculate percentage of total sales for each location and category
pct_sales_age = total_sales_post / total_sales_post.sum()
pct_sales_category = total_sales_category / total_sales_category.sum()


# Step 3: Calculate correlation between sales for each category and location
df_ = pd.read_csv(r'C:\Users\roy_shaw\Desktop\Work_Files\Campaign Analysis\Cleaned Data-OECS\FY23\corr_correct_loc_OECS.csv', index_col = 0)

lst_cat = ['ACCESSORIES AND OTHERS', 'APPLIANCES FURNITURE','ASHLEY BEDDING', 'ASHLEY DINING',           'ASHLEY ENTERTAINMENT','ASHLEY MASTER BEDROO', 'ASHLEY MOTION', 'ASHLEY OCCASIONAL', 'ASHLEY STATIONARY',           'AUDIO', 'AUDIO RSK', 'BABY GOODS','BEDDING', 'BEDROOM', 'BICYCLES', 'BLANK MEDIA', 'CAMPING','CAR AUDIO',           'CELL PHONES','CELLPHONES AND ACCESSORIES', 'COMPUTER', 'COMPUTER AND ACCESSORIES ', 'COOLING AND HEATING',           'DECORATIVE ACCESSORIES ', 'DIGITAL STORAGE', 'DINING', 'ELECTRICAL GENERATORS', 'FRIDGE',           'FURNITURE OCCASSIONAL MISC', 'GAMES AND TOYS', 'GAMING', 'GAMING ACCESSORIES', 'GAMING ACCESSORIES',           'GAMING CONSOLES', 'GAMING PCS', 'HARDWARE', 'HEADPHONES EARPHONES', 'HEALTH', 'HOME AND OFFICE', 'HOUSEWARES',           'INDIVIDUAL SPORT', 'KITCHEN', 'LAPTOPS', 'LENSES', 'LOUGE', 'MUSICAL INSTRUMENTS', 'OFFICE',           'OFFICE SYSTEMS', 'OPTICAL ACCESSORIES', 'OTHERS RSK PRODUCTS', 'OUTDOOR', 'OUTDOOR SPORT', 'PERSONAL CARE',           'PREMIUM FRAMES', 'PRINTERS', 'REGULAR FRAMES', 'REPO ELECTRICAL', 'REPO FURNITURE', 'SEWING MACHINES'           'SMALL APPLIANCES', 'SPARE PARTS FOR ELECTRICAL', 'STORAGE AND ORGANIZATION', 'STOVES', 'SUNGLASSES',           'TABLETS', 'TRANSPORT', 'TV AND ACCESSORIES', 'VEHICLES ACCESSORIES', 'VENTILATION', 'VIDEO GAMES',           'VISION', 'WASHING MACHINE AND DRYER']
lst_branch = list(df_.columns)
lst_corr = []
lst_obj_i = []
lst_obj_j = []
for i in range(len(lst_cat)):
    lst_obj_i += [lst_cat[i]]*39
    for j in range(len(lst_post)):
        lst_obj_j.append(lst_post[j])
lst_corr = df_.values.tolist()

data = {'category':[i for i in lst_obj_i],
       'post_code':[i for i in lst_obj_j],
       'corr':[x for l in lst_corr for x in l]}

df_new = pd.DataFrame(data)

df_new.set_index(['category', 'post_code'], inplace=True)

# convert the index to a list of tuples and then to a dictionary
correlations = df_new.index.tolist()
correlations = {key: value for key, value in zip(correlations, df_new['corr'])}

# Step 4: Define decision variables
categories = df_agg_main_HP['ItemCategory'].unique()
post_groups = df_agg_main_HP['PostCode'].unique()
allocation = LpVariable.dicts("allocation", [(category, post_code) for category in categories for post_code in post_groups], lowBound=0, cat='Continuous')

# Step 5: Set up linear programming model
model = LpProblem("Retail Category Allocation", LpMaximize)

# Objective function
model += lpSum([correlations[(category, post_group)] * allocation[(category, post_group)] * total_sales_post[post_group] for post_group in post_groups for category in categories if category and (category, post_group) in correlations])

# Constraints
for post_group in post_groups:
    model += lpSum([allocation[(category, post_group)] for category in categories]) == pct_sales_age[post_group] * total_sales_post.sum()
for category in categories:
    model += lpSum([allocation[(category, post_group)] for post_group in post_groups]) >= 1e-6

# Step 6: Solve the optimization problem
model.solve()

# Print the optimal allocation of retail categories to age groups
for age_group in age_groups:
    print(f"Post Group {post_group}:")
    for category in categories:
        print(f"    {category}: {allocation[(category, post_group)].varValue}")


# ## Approach Alpha - CashLoan

# In[2]:


from functools import cache


# In[6]:


class Asset:
    pass


# In[5]:


def get_log_period_margin(margin_history: pd.DataFrame):
    close = margin_history['Margin'].values 
    return np.log(close[1:] / close[:-1]).reshape(-1, 1)


# daily_price_history has to at least have a column, called 'Margin'

class Asset:
    def __init__(self, name: str, margin_history: pd.DataFrame):
        self.name = name
        self.monthly_returns = get_log_period_margin(margin_history)
        self.expected_monthly_return = np.mean(self.monthly_returns)
          
    @property
    def expected_return(self):
        return Monthly_Margins_Count * self.expected_monthly_return

    def __repr__(self):
        return f'<Asset name={self.name}, expected return={self.expected_return}>'

    @staticmethod
    @cache
    def covariance_matrix(assets: Tuple[Asset]):  # tuple for hashing in the cache
        product_expectation = np.zeros((len(assets), len(assets)))
        for i in range(len(assets)):
            for j in range(len(assets)):
                if i == j:
                      product_expectation[i][j] = np.mean(assets[i].monthly_returns * assets[j].monthly_returns)
                else:
                      product_expectation[i][j] = np.mean(assets[i].monthly_returns @ assets[j].monthly_returns.T)
    
        product_expectation *= (Monthly_Margins_Count - 1) ** 2
        expected_returns = np.array([asset.expected_return for asset in assets]).reshape(-1, 1)
        product_of_expectations = expected_returns @ expected_returns.T
        return product_expectation - product_of_expectations

def random_weights(weight_count):
    weights = np.random.random((weight_count, 1))
    weights /= np.sum(weights)
    return weights.reshape(-1, 1)

w_initial = [0.4567, 0.1122, 0.0436, 0.0243, 0.0645, 0.0258, 0.0061, 0.1303, 0.0371, 0.0113, 0.0739]

class Portfolio:
    def __init__(self, assets: Tuple[Asset]):
        self.assets = assets
        self.asset_expected_returns = np.array([asset.expected_monthly_return for asset in assets]).reshape(-1, 1)
        self.covariance_matrix = Asset.covariance_matrix(assets)
        self.weights = random_weights(len(assets))
    
    def unsafe_optimize_with_risk_tolerance(self, risk_tolerance: float):
        res = minimize(
            lambda w: self._variance(w) - risk_tolerance * self._expected_return(w), random_weights(self.weights.size),
            constraints=[{'type': 'eq', 'fun': lambda w: np.sum(w) - 1.},],
            bounds=[(0., 1.) for i in range(self.weights.size)])
        assert res.success, f'Optimization failed: {res.message}'
        self.weights = res.x.reshape(-1, 1)
    
    def optimize_with_risk_tolerance(self, risk_tolerance: float):
        assert risk_tolerance >= 0.
        return self.unsafe_optimize_with_risk_tolerance(risk_tolerance)
  
    def optimize_with_expected_return(self, expected_portfolio_return: float):
        res = minimize(lambda w: self._variance(w), random_weights(self.weights.size),
        constraints=[
            {'type': 'eq', 'fun': lambda w: np.sum(w) - 1.},
            {'type': 'ineq', 'fun': lambda w: self._expected_return(w) - expected_portfolio_return},
            ], bounds=[(0., 1.) for i in range(self.weights.size)])
        assert res.success, f'Optimization failed: {res.message}'
        self.weights = res.x.reshape(-1, 1)
    
    def _expected_return(self, w):
        return (self.asset_expected_returns.T @ w.reshape(-1, 1))[0][0]
  
    def _variance(self, w):
        return (w.reshape(-1, 1).T @ self.covariance_matrix @ w.reshape(-1, 1))[0][0]

    @property
    def expected_return(self):
        return self._expected_return(self.weights)
  
    @property
    def variance(self):
        return self._variance(self.weights)

    def __repr__(self):
        return f'<Portfolio assets={[asset.name for asset in self.assets]}, expected return={self.expected_return}, variance={self.variance}>'


# In[ ]:


def margins_retrieve_data(categories: List[str], df_values: pd.DataFrame):
    dataframes = []
    for cat_name in categories:
        val_margins = df_values[df_values['Category'] == cat_name]
        dataframes.append(val_margins)
    return dataframes


# In[ ]:


stock_supply = ['Major White', 'Bedding', 'Glass and Mirrors', 'Business Equipment', 'Vision', 'Small Appliances',                 'Bedroom', 'Radio Shack', 'Occasional/Misc.', 'PCs & Notebooks', 'Lounge', 'Dining Room']

os.chdir('C:\\Users\\roy_shaw\\Desktop\\Work_Files\\Campaign Analysis\\Cleaned Data-OECS')
margins_dataframes = pd.read_excel('margin_data.xlsx', sheet_name = 'OECS FY20-FY23', header = 0)

split_margin_dataframes = margins_retrieve_data(stock_supply, margins_dataframes)

assets = tuple([Asset(name, margin_df) for name, margin_df in zip(stock_supply, split_margin_dataframes)])


# In[ ]:


assets.__repr__()


# In[ ]:


X = []
y = []

# Drawing random portfolios
for i in range(10000):
    portfolio = Portfolio(assets)
    X.append(np.sqrt(portfolio.variance))
    y.append(portfolio.expected_return)

plt.scatter(X, y, label='Random portfolios')

# Drawing the efficient frontier
X = []
y = []
for rt in np.linspace(-300, 200, 1000):
    portfolio.unsafe_optimize_with_risk_tolerance(rt)
    X.append(np.sqrt(portfolio.variance))
    y.append(portfolio.expected_return)

plt.plot(X, y, 'k', linewidth=3, label='Efficient frontier')

# Drawing optimized portfolios
portfolio.optimize_with_risk_tolerance(0)
plt.plot(np.sqrt(portfolio.variance), portfolio.expected_return, 'm+', markeredgewidth=5, markersize=20, label='optimize_with_risk_tolerance(0)')

portfolio.optimize_with_risk_tolerance(20)
plt.plot(np.sqrt(portfolio.variance), portfolio.expected_return, 'r+', markeredgewidth=5, markersize=20, label='optimize_with_risk_tolerance(20)')

portfolio.optimize_with_expected_return(0.002)
plt.plot(np.sqrt(portfolio.variance), portfolio.expected_return, 'g+', markeredgewidth=5, markersize=20, label='optimize_with_expected_return(0.002)')

plt.xlabel('Portfolio standard deviation')
plt.ylabel('Portfolio expected (logarithmic) return')
plt.legend(loc='lower right')
plt.show()


# In[ ]:


pd.options.display.float_format = "{:,.5f}".format

portfolio = Portfolio(assets)
portfolio.optimize_with_risk_tolerance(0)
riskless_weights = portfolio.weights.flatten()

portfolio.optimize_with_risk_tolerance(20)
weights_risk_tolerance = portfolio.weights.flatten()

portfolio.optimize_with_expected_return(0.002)
weights_return = portfolio.weights.flatten()

display(
  pd.DataFrame(
    list(
      zip(
        [asset.name for asset in portfolio.assets],
        riskless_weights,
        weights_risk_tolerance,
        weights_return
      )
    ),
    columns=[
      'asset',
      'optimize_with_risk_tolerance(0)',
      'optimize_with_risk_tolerance(20)',
      'optimize_with_expected_return(0.002)'
    ],
  )
)


# ## Ruffie

# In[ ]:


import statsmodels.api as sm
import pylab as py
from sklearn import preprocessing

lst_data_points = list(df_agg_main['AgreementTotal'])
normalized_arr = preprocessing.normalize([lst_data_points])
usable_df = pd.DataFrame(list(*normalized_arr))
  
sm.qqplot(usable_df, line ='45')
py.show()


# In[ ]:


usable_df.describe()


# In[ ]:


os.chdir(r'C:\\Users\\roy_shaw\\Desktop\\Work_Files\\Campaign Analysis\\Cleaned Data-OECS')
filler_conv.to_excel('agg_file_front_adj.xlsx', sheet_name = 'Sheet1')
df_prod_camp_key.to_excel('agg_main_adj.xlsx', sheet_name = 'Sheet1')


# In[ ]:


dict_metrics = dict(df_data_series_1['ItemCategory'].value_counts())


# In[ ]:


df_dict_items = pd.DataFrame([dict_metrics])


# In[ ]:


df_dict_items.head()


# In[ ]:


matrix = df_data_series_1['Age Group'].corr(df_dict_items)


# In[ ]:


matrix = df_data_series_1['ItemCategory'].corr(df_data_series_1['ItemCategory'])
print(matrix)


# ### Sample t-test on Quantitative attributes

# In[ ]:


random.seed(128)
Tkup = 'Takeup'
df_accept = ''
df_refuse = ''
for typ in list(df_jun[Tkup].dropna().unique()):
    df_refuse = df_jun[df_jun[Tkup] == 0].sample(1695)
    df_accept = df_jun[df_jun[Tkup] == 1].sample(1695)


# In[ ]:


numericColumns = ['ArrearsUSD', 'RFCreditLimit', 'USDAvailableSpend', 'RfavailablePercent']
corrMatrix = df_accept.loc[:,numericColumns].corr()
print(corrMatrix)
sns.heatmap(corrMatrix, annot=True)


# In[ ]:


a = df_refuse['RfavailablePercent']
b = df_accept['RfavailablePercent']
stats.ttest_rel(a, b)


# In[ ]:


x = df_refuse['Customer Lifetime']
y = df_accept['Customer Lifetime']
stats.ttest_rel(x, y)


# We deduce that the values for "Customer Lifetime" there is no evidence to show this variable being statistically significant to the decision for the uptake of the campaign.

# In[ ]:


k = df_refuse['Age']
l = df_accept['Age']
stats.ttest_rel(k, l)


# In[ ]:


plt.figure(figsize=[10,6])
sns.boxplot(x = 'Campaign', y = 'RfavailablePercent', data = df_accept, palette=my_pal).set(title='Boxplot of RfavailablePercentage segmented by Campaign Code for customers who took up the offer')


# In[ ]:


plt.figure(figsize=[10,6])
sns.boxplot(x = 'Campaign', y = 'Age', data = df_accept).set(title='Boxplot of Age segmented by Campaign Code for customers who took up the offer')


# In[ ]:


df_accept.groupby('Campaign')['RfavailablePercent','Age','Customer Lifetime'].median()


# In[ ]:


plt.figure(figsize=[10,6])
sns.boxplot(x = 'Campaign', y = 'RfavailablePercent', data = df_refuse, palette=my_pal).set(title='Boxplot of RfavailablePercentage segmented by Campaign Code for customers who did not take up the offer')


# In[ ]:


plt.figure(figsize=[10,6])
sns.boxplot(x = 'Campaign', y = 'Age', data = df_refuse).set(title='Boxplot of Age segmented by Campaign Code for customers who did not take up the offer')


# In[ ]:


df_refuse.groupby('Campaign')['RfavailablePercent', 'Age', 'Customer Lifetime'].median()


# ### ANOVA Test for Categorical Variables and uptake

# In[ ]:


stats.f_oneway(df_jun['Takeup'][df_jun['Occupation'] == 'BLANK'],
               df_jun['Takeup'][df_jun['Occupation'] == 'Professional'],
               df_jun['Takeup'][df_jun['Occupation'] == 'Clerical Support Workers'],
               df_jun['Takeup'][df_jun['Occupation'] == 'Elementary Occupations'],
               df_jun['Takeup'][df_jun['Occupation'] == 'Service and Sales Workers'],
               df_jun['Takeup'][df_jun['Occupation'] == 'Other'],
               df_jun['Takeup'][df_jun['Occupation'] == 'Technicians & Associate Professionals'],
               df_jun['Takeup'][df_jun['Occupation'] == 'Managers'],
               df_jun['Takeup'][df_jun['Occupation'] == 'Pensioner'],
               df_jun['Takeup'][df_jun['Occupation'] == 'Skilled Agriculture/Forestry/Fishery Workers'],
               df_jun['Takeup'][df_jun['Occupation'] == 'Factory Workers'],
               df_jun['Takeup'][df_jun['Occupation'] == 'Craft and Related Trades Workers'],
               df_jun['Takeup'][df_jun['Occupation'] == 'Plant/Machine Operators/Assemblers'])


# The low p-value above satisfies the conculsion that the categorical variable associated with Occupation does in fact affect our Customer's Campaign take-up.

# In[ ]:


stats.f_oneway(df_jun['Takeup'][df_jun['maritalstat'] == 'S'],
               df_jun['Takeup'][df_jun['maritalstat'] == 'M'],
               df_jun['Takeup'][df_jun['maritalstat'] == 'C'],
               df_jun['Takeup'][df_jun['maritalstat'] == 'D'],
               df_jun['Takeup'][df_jun['maritalstat'] == 'P'],
               df_jun['Takeup'][df_jun['maritalstat'] == 'W'])               


# The low p-value above satisfies the conclusion that Marital Status has an impact on the customer's Campaign Take-up.

# In[ ]:


stats.f_oneway(df_jun['Takeup'][df_jun['sex'] == 'M'],
               df_jun['Takeup'][df_jun['sex'] == 'F'])


# We observe from the low p-value that there is insufficient evidence to deduce that the gender of the customer does not influence the customer's decision to take-up the offer.

# We have now found our variables of interest for this analysis namely "USDAvailableSpend", "ArrearsUSD", "RFCreditLimit", "Age" for the Quantitative Variables and "Occupation" and "maritalstat", we may proceed to conduct the analysis. Also as the numerical variables for "USDAvailableSpend", "ArrearsUSD" and "RFCreditLimit" have a significant amount of outliers we will have to reduce these values.
# 
# We have significant outliers which would affect the accuracy of the model, we may normalize/group to reduce the impact of these values, we will use the separation method and consider three insances. Customer owes more than the lower bound of the interquartile range, customer within the bounds of the interquartile ranges and customer is over the upper bound of the interquartile range.

# Properties of the standard normal distribution:
#     50% - 0.00,
#     75% - 0.675,
#     90% - 1.282,
#     95% - 1.645,
#     97.5% - 1.96,
#     99% - 2.326

# ### Analysis of results

# #### Sex

# In[ ]:


sns.countplot(y = 'Campaign', data = df_refuse)


# In[ ]:


sns.countplot(y = 'Campaign', data = df_accept)


# In[ ]:


df_plot = df_refuse.groupby(['sex', 'Campaign']).size().reset_index().pivot(columns='sex', index='Campaign', values=0)


# In[ ]:


df_plot.plot(kind='bar', stacked=True)


# In[ ]:


df_plot_acc = df_accept.groupby(['sex', 'Campaign']).size().reset_index().pivot(columns='sex', index='Campaign', values=0)


# In[ ]:


df_plot_acc.plot(kind='bar', stacked=True)


# In[ ]:


'''lst_RFCL = list(df_accept['RFCreditLimit'])
lst_corr = []
for i in range(len(df_accept['RFCreditLimit'])):
    lst_corr.append(lst_RFCL[i]/2.70)
df_jun['RFCreditLimit'] = lst_corr'''    


# In[ ]:


df_jun.pivot_table(index = 'Spend Category', aggfunc = 'mean')


# In[ ]:


df_jun.pivot_table(index = 'Takeup_Bool', aggfunc = 'mean')


# In[ ]:


sns.countplot(y = 'Campaign', data = df_jun)


# In[ ]:


sns.histplot(data=df_jun, x="Campaign", hue = "Takeup_Bool", label="Campaign Acceptance", kde=False)


# Recall the Spend Category is the quantile ranges for 'USDAvailableSpend' variable so the data in the pivot table above gives the mean values for the remaining numerical variables in the analysis. Of interest are the RfavailablePercent and Takeup_Bool (which is an indicator of whether the targetted customer took up the offer converting from the lead to a conversion. as we see the highest RF Availabilities on average are for Spend Category 1 and 5, which represent the 20th percentile (maxima) an the 100th percentile maxima. Customers which had the highest Take Up (on average) were those in the 80th (maxima) Spend Category percentile.

# In[ ]:


rain = (~(df_active['ArrearsUSD']<(Q1_a-1.5*IQR_a)) & ~(df_active['ArrearsUSD']>(Q3_a+1.5*IQR_a)))
df_active['ArrearsUSD_outlier_Bool'] = rain
df_active.drop(df_active.index[df_active['ArrearsUSD_outlier_Bool'] != True], axis = 0, inplace = True)


# Spitting the df_jun dataframe into standard, positive outliers and negative outliers i.e 
# 
# Separating the main Dataframe for Jun (df_jun) into three sub-dataframes to account for the outliers. These dataframes represent the onbservations which are within 1.5 s.d. of the mean, those which are outliers on the negative end of the range and those which outlie on the positive end of the range.

# # Machine Learning Component

# In[ ]:


class MultiColumnLabelEncoder:
    
    def __init__(self, columns = None):
        self.columns = columns # list of column to encode    
        
    def fit(self, X, y=None):
        return self    
    def transform(self, X):
        '''
        Transforms columns of X specified in self.columns using
        LabelEncoder(). If no columns specified, transforms all
        columns in X.
        '''
        
        output = X.copy()
        
        if self.columns is not None:
            for col in self.columns:
                output[col] = LabelEncoder().fit_transform(output[col])
        else:
            for colname, col in output.iteritems():
                output[colname] = LabelEncoder().fit_transform(col)
        
        return output    
    
    def fit_transform(self, X, y=None):
        return self.fit(X, y).transform(X)


# In[ ]:


le = MultiColumnLabelEncoder(['Campaign_int', 'Occupation_int'])
df_beta = le.transform(df_jun)
#df_beta = le.fit_transform(df_jun['Campaign'])
df_beta.head()


# In[ ]:


df_beta.groupby(['Occupation', 'Occupation_int']).groups.keys()


# In[ ]:


df_beta.groupby(['Campaign', 'Campaign_int']).groups.keys()


# In[ ]:


oe_style = OneHotEncoder(sparse=False, handle_unknown='ignore')
oe_occu_results = oe_style.fit_transform(df_beta[['Occupation_int']])
oe_camp_results = oe_style.fit_transform(df_beta[['Campaign_int']])
#df_alpha = pd.DataFrame(oe_results.toarray(), columns=oe_style.categories_)


# In[ ]:


oe_occu_results.shape


# In[ ]:


df_alpha.reset_index(drop=True)


# In[ ]:


df_jun_cleaned = df_jun.join(df_alpha)

#pd.concat([df_jun, df_alpha], axis=1)


# In[ ]:


df_jun['integerized_data_Occupation'] = integerized_data


# In[ ]:


df_jun[['Occupation','integerized_data_Occupation']]


# In[ ]:


df_jun


# ## Model Building

# In[ ]:


key = df_jun['Occupation'].unique() & df_jun['integerized_data_Occupation']
print(key)


# In[ ]:


#df_main.to_excel(r'C:\Users\roy_shaw\Desktop\Complete_Ticketing.xlsx', 'Report', index = False, encoding = 'utf-8')

