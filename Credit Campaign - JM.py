#!/usr/bin/env python
# coding: utf-8

# # Campaign Successes Analysis - Jamaica

# In[2]:


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


# In[53]:


server = '10.192.26.32'
database = 'CODS'
username = 'roy_shaw'
password = 'SpriteVvs#41605***'
cnxn = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};""Server=10.192.26.32;""Database=CODS;""trusted_connection=yes;""UID=roy_shaw;""PWD=SpriteVvs#41605***;")
cursor = cnxn.cursor()


# #### Change Date Here

# In[54]:


df_campaign_success = pd.read_sql("SELECT * FROM CODS.dbo.[Credit.BookingsAndDeliveriesMar2023]",cnxn)


# In[55]:


filler_1 = df_campaign_success


# In[56]:


filler_1.rename(columns = {'Customer ID':'MasterIDNumber','Account Number':'Acctno'}, inplace = True)


# #### Change Date Here

# In[57]:


df_Control = pd.read_sql("SELECT * FROM CODS.dbo.[RPT_CL_HP_CUSTS_ACCT_SMRY] WHERE DateAcctOpen > '2023-03-01'",cnxn)


# In[58]:


filler_2 = df_Control


# In[59]:


filler_2.rename(columns = {'Custid':'MasterIDNumber', 'Account_Number':'Acctno'}, inplace = True)


# #### Change Date Here

# In[60]:


df_Master_Purchases = pd.read_sql("SELECT [CountryCustomerID], [MasterIDNumber], [Account Number] ,[CampaignCode], [Agreement Total], [Country], [Account Type], [Customer ID], [Date Account Opened], [Delivered Disbursed Date], [Delivery Status], [ISO], [Terms Type] FROM CODS.dbo.[vwCampaignSuccessMar2023]".format('db'),cnxn)


# In[61]:


filler_3 = df_Master_Purchases


# In[62]:


filler_3.rename(columns = {'Account Number':'Acctno'}, inplace = True)


# In[63]:


filler_3.drop(['Delivered Disbursed Date'], axis = 1, inplace = True)


# #### Change Date Here

# In[64]:


df_main = pd.read_sql("SELECT * FROM [CODS].[dbo].[Credit.CampaignListMar2023]",cnxn)


# In[65]:


filler_4 = df_main


# In[66]:


filler_4 = filler_4.merge(filler_1, on = 'MasterIDNumber', how = 'inner')


# In[67]:


filler_4['Take-up Bool'] = filler_4['MasterIDNumber'].isin(filler_3['MasterIDNumber'])


# Pre-FY23

# In[68]:


'''filler_4.drop(['Firstname', 'LastName', 'RFCreditLimit', 'AvailableSpend', 'HomeTel', 'WorkTel',              'MobileTel','OutstandingBalance', 'AgreementTotal', 'USDAvailableSpend',               'MostRecentAccount_x', 'Agreement Total', 'Service Charge', 'Principle Amount',              'Delivery Status', 'Country_y', 'ISO_y'], axis = 1, inplace = True)'''


# In[69]:


filler_4.columns


# Post-FY23

# In[70]:


filler_4.drop(['firstname', 'name', 'RFCreditLimit', 'AvailableSpend', 'Arrears', 'HomeTel', 'WorkTel', 'MobileTel',              'OutstandingBalance', 'AgreementTotal', 'USDAvailableSpend', 'ArrearsUSD', 'DateSettled', 'MostRecentAccount',              'Delivered Disbursed Date', 'Agreement Total', 'Service Charge', 'Principle Amount', 'Terms Type', 'Account Type',              'Delivery Status', 'Country_y', 'ISO_y'],              axis = 1, inplace = True)


# In[71]:


filler_4['Clean_Order'] = filler_4['ISO_x'].isin(['JM'])
filler_4.drop(filler_4['Clean_Order'].index[filler_4['Clean_Order'] == False], axis = 0, inplace = True)


# In[72]:


df_tkup_success = filler_4[filler_4['Take-up Bool'] == True]


# In[73]:


df_tkup_success = df_tkup_success.rename(columns = {'MostRecentAccount_y':'Acctno'})


# In[74]:


df_tkup_fail = filler_4[filler_4['Take-up Bool'] == False]


# In[75]:


df_prod_camp_key = df_tkup_success.merge(filler_2, on = 'Acctno', how = 'outer')


# In[76]:


df_prod_camp_key = df_prod_camp_key.dropna(subset = ['CountryCustomerID'])
df_prod_camp_key = df_prod_camp_key.dropna(subset = ['MasterIDNumber_y'])


# In[77]:


df_prod_camp_key = df_prod_camp_key.merge(filler_3, on = 'Acctno', how = 'inner')


# In[78]:


df_prod_camp_key.drop(['Account Type','MasterIDNumber_y', 'MasterIDNumber_x', 'Agreement Total',                       'FirstName','LastName','RFCreditLimit','AvailableSpend','OutstandingBalance',                       'Arrears','No_of_CLN','No_of_HP','CLN_Agrtotal','HP_Agrtotal', 'ServiceCharge',                       'TotalInstalments', 'ISO_x', 'WorstStatusCode','MthlyIncome','Birthdate','Gender','Age_y',                       'Occupation_y','CountryCustomerID_y','Agreement Total', 'MostRecentAcctno',                       'Country_x','Date Account Opened_y','Take-up Bool','Clean_Order',                       'Date Account Opened_x', 'Is_Cashloan_Acct','Is_HPorRF_Acct','Is_Both','Item','ItemType',                       'ItemCatCode', 'ServiceCharge', 'DivisionCode',  'DepartmentCode',                        'ItemNo2', 'ClassCode', 'ClassID', 'ClassName', 'RFCreditLimit_USD', 'AvailableSpend_USD',                       'OutstandingBalance_USD', 'Arrears_USD', 'CLN_Agrtotal_USD', 'HP_Agrtotal_USD',                       'TotalInstalments_USD', 'MthlyIncome_USD', 'ServiceCharge_USD', 'DatasetLastUpdated', 'HomeTel',                       'MobileTel', 'WorkTel', 'Email', 'Country_y', 'CustAddr1', 'CustAddr2', 'CusPOCode', 'Country_y'], 
                      axis = 1, inplace = True)


# In[79]:


'''lst_Rcnt_Ac = df_prod_camp_key['MostRecentAccount']
lst_Brnch_Num = []
for i in range(len(lst_Rcnt_Ac)):
    lst_Brnch_Num.append(lst_Rcnt_Ac[i][:3])
df_prod_camp_key['StoreID'] = lst_Brnch_Num'''


# In[80]:


df_prod_camp_key = df_prod_camp_key[['CountryCustomerID_x','MasterIDNumber','ISO_y', 'DivisionName', 'DepartmentName',                                     'RfavailablePercent','Acctno', 'Is_Cancelled','DateAccOpenMth','DateAcctOpen',                                     'AgreementTotal_USD','CampaignCode_x','CampaignCode_y', 'DeliveryFlag', 'DeliveryDate',                                     'Itemno', 'ItemCategory','Terms Type', 'BranchName','dateborn',                                     'Age_x','sex','maritalstat','Occupation_x']]


# In[81]:


df_prod_camp_key.drop_duplicates(subset = ['CountryCustomerID_x','CampaignCode_x','CampaignCode_y'], keep =                                           'first', inplace = True)


# In[82]:


'''os.chdir('C:\\Users\\roy_shaw\\Desktop\\Work_Files\\Campaign Analysis')
df_store = pd.read_excel('Store_Finder-OECS.xlsx', sheet_name = 'Store_Finder-JM')
lst_StoreID = list(df_store['StoreID'])
new_StoreID = []
df_store.drop('StoreID', axis = 1, inplace = True)
for i in range(len(lst_StoreID)):
    new_StoreID.append(str(lst_StoreID[i]))
df_store['StoreID'] = new_StoreID
df_prod_camp_key = df_prod_camp_key.merge(df_store, how = 'inner', on = 'StoreID')'''


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


df_prod_camp_key = df_prod_camp_key.T.drop_duplicates().T


# In[89]:


df_prod_camp_key = df_prod_camp_key[df_prod_camp_key['DeliveryFlag'] != 'N']


# In[90]:


filler_conv = df_main[df_main['ISO'] == 'JM']


# In[91]:


filler_conv['Conversion'] = filler_conv['CountryCustomerID'].isin(df_prod_camp_key['CountryCustomerID_x'])


# In[92]:


filler_conv['Conversion'].value_counts()


# ##### Change Date Here

# In[93]:


import datetime
date_ = datetime.date(2023, 3, 1)


# #### Important! resurface this input if required

# In[94]:


'''cust_life = []
trans_date = list(pd.to_datetime(df_prod_camp_key['EarliestDateAcctOpen']).apply(lambda x: x.date()))
end_date_cur = [date_]*len(trans_date)
for trn,end in zip(trans_date, end_date_cur):
    delta = (end-trn).days/365.25
    cust_life.append(delta)
df_prod_camp_key['Customer Lifetime'] = cust_life'''

#df_prod_camp_key['Customer Lifetime']


# In[95]:


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


# In[96]:


df_prod_camp_key['Occupation_x'].unique()


# In[97]:


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


# In[98]:


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


# In[99]:


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


# In[100]:


from tabulate import tabulate
table = [['Generation Label', 'Age-Group'], ['Gen Z','18-25'],['Millenial','26-41'],['Gen X','42-57'],['Boomers 2','58-67'],['Boomers 1','68-76'],['Post War','77-94'],['WW2','>95']]
print(tabulate(table, headers = "firstrow"))


# #### Change date here

# In[101]:


df_prod_camp_key['YearMonth'] = ['202303']*len(df_prod_camp_key['CountryCustomerID_x'])
filler_conv['YearMonth'] = ['202303']*len(filler_conv['CountryCustomerID'])


# #### Change date here

# In[102]:


with pd.ExcelWriter(r'C:\Users\roy_shaw\Desktop\output_Mar_FY23_JM.xlsx') as writer:
    df_prod_camp_key.to_excel(writer, sheet_name = 'Main')
    filler_conv.to_excel(writer, sheet_name = 'Aggregate')


# In[108]:


lst_file_name = []
df_agg_main = pd.DataFrame()
df_agg_supp = pd.DataFrame()
directory = r"C:\Users\roy_shaw\Desktop\Work_Files\Campaign Analysis\Cleaned Data-JM\FY22-23"
for filename in os.scandir(f'{directory}'):
    if filename.is_file():
        lst_file_name.append(filename.path[-23:-5])
lst_set_file_name = list(set(lst_file_name))
for i in range(len(lst_set_file_name)):
    curr_df = pd.read_excel(f'{directory}\{lst_set_file_name[i]}.xlsx', sheet_name = 'Main')
    post_df = pd.read_excel(f'{directory}\{lst_set_file_name[i]}.xlsx', sheet_name = 'Aggregate')
    df_agg_main = pd.concat([df_agg_main, curr_df])
    df_agg_supp = pd.concat([df_agg_supp, post_df])
    curr_df = pd.DataFrame()
    post_df = pd.DataFrame()

df_front = df_agg_main.to_csv(r'C:\\Users\\roy_shaw\\Desktop\\Work_Files\\Campaign Analysis\\df_main_agg_JM.csv', index = False, encoding = 'utf-8')
df_escrt = df_agg_supp.to_csv(r'C:\\Users\\roy_shaw\\Desktop\\Work_Files\\Campaign Analysis\\df_agg_totl_JM.csv', index = False, encoding = 'utf-8')           


# In[3]:


os.chdir('C:\\Users\\roy_shaw\\Desktop\\Work_Files\\Campaign Analysis')
df_agg_main = pd.read_csv('df_main_agg_JM.csv')


# In[4]:


exclude_items = ['DT', 'ADMIN', 'LOAN', 'PPP', 'SD', 'RASTP1', 'RASTB3']
df_agg_main_HP = df_agg_main[~df_agg_main['Itemno'].isin(exclude_items)]


# In[5]:


df_agg_main_HP['ItemCategory'].value_counts()


# In[6]:


df_agg_main_HP['DepartmentName'].value_counts().to_csv(r'C:\Users\roy_shaw\Desktop\categories_ttl_JM.csv')


# In[7]:


lst_prod = list(df_agg_main_HP['DepartmentName'])
lst_cleaned = []
for i in range(len(lst_prod)):
    if lst_prod[i] != np.nan:
        lst_cleaned.append(str(lst_prod[i]).strip())
    else:
        continue
df_agg_main_HP['DepartmentName'] = lst_cleaned


# In[8]:


df_agg_main_HP['DepartmentName'].unique()


# In[10]:


df_agg_main = df_agg_main[df_agg_main['Is_Cancelled'] == 'N']


# In[48]:


df_agg_main_HP['BranchName'].unique()


# In[11]:


'''from matplotlib import rcParams
rcParams['figure.figsize'] = 12, 9'''


# In[12]:


#Monthly_Margins_Count = 12


# ## Approach Alpha - HP

# #### Probability Matrix by Age

# In[22]:


dept_type = ['WASHING MACHINE AND DRYER', 'SMALL APPLIANCES', 'FRIDGE', 'STOVES', 'OPTICAL ACCESSORIES',              'AUDIO', 'LOUNGE', 'LENSES', 'BEDDING', 'DINING', 'LAPTOPS', 'VISION', 'BEDROOM', 'OUTDOOR',              'KITCHEN', 'INDIVIDUAL SPORT', 'ACCESSORIES AND OTHERS', 'DECORATIVE ACCESSORIES', 'TV AND ACCESORIES',              'COMPUTER', 'ASHLEY STATIONARY', 'CELLPHONES AND ACCESSORIES', 'REGULAR FRAMES', 'VENTILATION',              'FURNITURE OCCASIONAL MISC', 'COMPUTER AND ACCESSORIES', 'AUDIO RSK', 'HARDWARE', 'HEADPHONES EARPHONES',              'OFFICE', 'BAGS', 'SEWING MACHINES', 'HOUSEWARES', 'ASHLEY DINING', 'ASHLEY MOTION', 'ASHLEY BEDDING',              'TABLETS', 'PREMIUM FRAMES', 'ASHLEY MASTER BEDROO', 'ASHLEY ENTERTAINMENT', 'CAMPING', 'OUTDOOR SPORT',             'STORAGE AND ORGANIZATION', 'ASHLEY ACCENTS', 'GAMING CONSOLES', 'SUNGLASSES', 'ASHLEY OUTDOOR',             'COOLING AND HEATING', 'SPARE PARTS FOR ELECTRICAL', 'FLOOR COVERINGS', 'HOME AND OFFICE', 'CONTACT LENSES',             'MUSIC', 'VEHICLES ACCESSORIES', 'ELECTRICAL GENERATORS', 'LIGHTING', 'ASHLEY RUGS', 'ASHLEY OCCASIONAL',             'REPO FURNITURE', 'SECURITY SYSTEM', 'BABY GOODS', 'ASHLEY SECTIONALS', 'PERSONAL CARE',             'ASHLEY HOME OFFICE', 'DIGITAL STORAGE', 'REPO ELECTRICAL', 'REPO PCS', 'TRANSPORT', 'HEALTH',             'BICYCLES', 'GAMING PCS', 'OFFICE SYSTEM', 'OTHERS RSK PRODUCTS', 'nan', 'MUSICAL INSTRUMENTS']

age_coeff = ['Gen Z', 'Millenial', 'Gen X', 'Boomers 2', 'Boomers 1', 'Post War', 'WW2']


# In[39]:


count_matrix = {}
for i in range(len(age_coeff)):
    for j in range(len(dept_type)):
        simp = (df_agg_main_HP['Age Group'] == age_coff[i]) & (df_agg_main_HP['DepartmentName'] == dept_type[j])
        val_ = df_agg_main[simp]['CountryCustomerID_x'].count()
        count_matrix[(age_coeff[i], dept_type[j])] = val_
        j+=1
    i+=1


# In[41]:


columns = list(set([key[0] for key in count_matrix.keys()]))
rows = list(set([key[1] for key in count_matrix.keys()]))


# In[44]:


df_count = pd.DataFrame(index=rows, columns=columns)
for key, value in count_matrix.items():
    df_count.loc[key[1], key[0]] = value


# In[45]:


df_count


# #### Correlation by Location

# In[126]:


rows= []

for var1 in df_encoded_1:
    col = []
    for var2 in df_encoded_1:
        cramers =cramers_v(df_encoded_1[var1], df_encoded_1[var2]) # Cramer's V test
        col.append(round(cramers,2)) # Keeping of the rounded value of the Cramer's V  
    rows.append(col)
    cramers_results = np.array(rows)
df2 = pd.DataFrame(cramers_results, columns = df_encoded_1.columns, index =df_encoded_1.columns)


# In[127]:


df.to_csv(r'C:\Users\roy_shaw\Desktop\temp_corr_age_JM_rev.csv')


# In[128]:


df2.to_csv(r'C:\Users\roy_shaw\Desktop\temp_corr_correct_loc_JM.csv')


# In[141]:


# Step 1: Calculate total sales for each age group and retail category
total_sales_age = df_agg_main_HP.groupby(['Age Group'])['AgreementTotal_USD'].sum()
total_sales_category = df_agg_main_HP.groupby(['DepartmentName'])['AgreementTotal_USD'].sum()

# Step 2: Calculate percentage of total sales for each age group and category
pct_sales_age = total_sales_age / total_sales_age.sum()
pct_sales_category = total_sales_category / total_sales_category.sum()

#df_agg_main_HP['ItemCategory'].unique()

# Step 3: Calculate correlation between sales for each category and age group
df_ = pd.read_csv(r'C:\Users\roy_shaw\Desktop\Work_Files\Campaign Analysis\Cleaned Data-JM\corr_age_JM_FY23.csv', index_col = 0)

lst_cat = ['WASHING MACHINE AND DRYER', 'SMALL APPLIANCES', 'FRIDGE', 'STOVES', 'OPTICAL ACCESSORIES', 'AUDIO',           'LOUNGE', 'LENSES', 'BEDDING', 'DINING', 'LAPTOPS', 'VISION', 'BEDROOM', 'OUTDOOR', 'KITCHEN',           'INDIVIDUAL SPORT', 'ACCESSORIES AND OTHERS', 'DECORATIVE ACCESSORIES', 'TV AND ACCESORIES', 'COMPUTER',           'ASHLEY STATIONARY', 'CELLPHONES AND ACCESSORIES', 'REGULAR FRAMES', 'VENTILATION',           'FURNITURE OCCASIONAL MISC', 'COMPUTER AND ACCESSORIES', 'AUDIO RSK', 'HARDWARE', 'HEADPHONES EARPHONES',           'OFFICE', 'BAGS', 'SEWING MACHINES', 'HOUSEWARES', 'ASHLEY DINING', 'ASHLEY MOTION', 'ASHLEY BEDDING',           'TABLETS', 'PREMIUM FRAMES', 'ASHLEY MASTER BEDROO', 'ASHLEY ENTERTAINMENT', 'CAMPING', 'OUTDOOR SPORT',           'STORAGE AND ORGANIZATION', 'ASHLEY ACCENTS', 'GAMING CONSOLES', 'SUNGLASSES', 'ASHLEY OUTDOOR',           'COOLING AND HEATING', 'SPARE PARTS FOR ELECTRICAL', 'FLOOR COVERINGS', 'HOME AND OFFICE', 'CONTACT LENSES',           'MUSIC', 'VEHICLES ACCESSORIES', 'ELECTRICAL GENERATORS', 'LIGHTING', 'ASHLEY RUGS', 'ASHLEY OCCASIONAL',           'REPO FURNITURE', 'SECURITY SYSTEM', 'BABY GOODS', 'ASHLEY SECTIONALS', 'PERSONAL CARE', 'ASHLEY HOME OFFICE',           'DIGITAL STORAGE', 'REPO ELECTRICAL', 'REPO PCS', 'TRANSPORT', 'HEALTH', 'BICYCLES', 'GAMING PCS',           'OFFICE SYSTEM', 'OTHERS RSK PRODUCTS', 'nan', 'MUSICAL INSTRUMENTS']
lst_age = list(df_.columns)
lst_corr = []
lst_obj_i = []
lst_obj_j = []
for i in range(len(lst_cat)):
    lst_obj_i += [lst_cat[i]]*74
    for j in range(len(lst_age)):
        lst_obj_j.append(lst_age[j])
lst_corr = df_.values.tolist()

print(len(lst_obj_i), len(lst_obj_j), len(lst_corr))

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
model += lpSum([correlations[(category, age_group)] * allocation[(category, age_group)] * total_sales_age[age_group] for age_group in age_groups for category in categories])

# Constraints
for age_group in age_groups:
    model += lpSum([allocation[(category, age_group)] for category in categories]) == pct_sales_age[age_group] * total_sales_age.sum()
for category in categories:
    model += lpSum([allocation[(category, age_group)] for age_group in age_groups]) >= 1e-6

# Step 6: Solve the optimization problem
model.solve()

# Create a dataframe to store the optimal allocation of retail categories to age groups
allocation_data = []
for age_group in age_groups:
    for category in categories:
        allocation_data.append({'Age Group': age_group, 'Category': category, 'Allocation': allocation[(category, age_group)].varValue})

allocation_df = pd.DataFrame(allocation_data)

# Reshape the dataframe
allocation_pivot = allocation


# In[142]:


allocation_df.to_csv(r'C:\Users\roy_shaw\Desktop\allocation_JM.csv')


# In[149]:


# Step 1: Calculate total sales for each location and retail category
total_sales_post = df_agg_main_HP.groupby(['BranchName'])['AgreementTotal_USD'].sum()
total_sales_category = df_agg_main_HP.groupby(['DepartmentName'])['AgreementTotal_USD'].sum()

# Step 2: Calculate percentage of total sales for each location and category
pct_sales_age = total_sales_post / total_sales_post.sum()
pct_sales_category = total_sales_category / total_sales_category.sum()


# Step 3: Calculate correlation between sales for each category and location
df_ = pd.read_csv(r'C:\Users\roy_shaw\Desktop\Work_Files\Campaign Analysis\Cleaned Data-JM\corr_loc_JM_FY23.csv', index_col = 0)

lst_cat = ['WASHING MACHINE AND DRYER', 'SMALL APPLIANCES', 'FRIDGE', 'STOVES', 'OPTICAL ACCESSORIES', 'AUDIO',           'LOUNGE', 'LENSES', 'BEDDING', 'DINING', 'LAPTOPS', 'VISION', 'BEDROOM', 'OUTDOOR', 'KITCHEN',           'INDIVIDUAL SPORT', 'ACCESSORIES AND OTHERS', 'DECORATIVE ACCESSORIES', 'TV AND ACCESORIES', 'COMPUTER',           'ASHLEY STATIONARY', 'CELLPHONES AND ACCESSORIES', 'REGULAR FRAMES', 'VENTILATION',           'FURNITURE OCCASIONAL MISC', 'COMPUTER AND ACCESSORIES', 'AUDIO RSK', 'HARDWARE', 'HEADPHONES EARPHONES',           'OFFICE', 'BAGS', 'SEWING MACHINES', 'HOUSEWARES', 'ASHLEY DINING', 'ASHLEY MOTION', 'ASHLEY BEDDING',           'TABLETS', 'PREMIUM FRAMES', 'ASHLEY MASTER BEDROO', 'ASHLEY ENTERTAINMENT', 'CAMPING', 'OUTDOOR SPORT',           'STORAGE AND ORGANIZATION', 'ASHLEY ACCENTS', 'GAMING CONSOLES', 'SUNGLASSES', 'ASHLEY OUTDOOR',           'COOLING AND HEATING', 'SPARE PARTS FOR ELECTRICAL', 'FLOOR COVERINGS', 'HOME AND OFFICE', 'CONTACT LENSES',           'MUSIC', 'VEHICLES ACCESSORIES', 'ELECTRICAL GENERATORS', 'LIGHTING', 'ASHLEY RUGS', 'ASHLEY OCCASIONAL',           'REPO FURNITURE', 'SECURITY SYSTEM', 'BABY GOODS', 'ASHLEY SECTIONALS', 'PERSONAL CARE', 'ASHLEY HOME OFFICE',           'DIGITAL STORAGE', 'REPO ELECTRICAL', 'REPO PCS', 'TRANSPORT', 'HEALTH', 'BICYCLES', 'GAMING PCS',           'OFFICE SYSTEM', 'OTHERS RSK PRODUCTS', 'MUSICAL INSTRUMENTS', 'Vogue_1', 'Vogue_2', 'Vogue_3', 'Vogue_4',           'Vogue_5', 'Vogue_6', 'Vogue_7', 'Vogue_8', 'Vogue_9', 'nan']
lst_post = list(df_.columns)
lst_corr = []
lst_obj_i = []
lst_obj_j = []
for i in range(len(lst_cat)):
    lst_obj_i += [lst_cat[i]]*83
    for j in range(len(lst_post)):
        lst_obj_j.append(lst_post[j])
lst_corr = df_.values.tolist()

print(len(lst_obj_i), len(lst_obj_j), len(lst_corr))

data = {'category':[i for i in lst_obj_i],
       'post_code':[i for i in lst_obj_j],
       'corr':[x for l in lst_corr for x in l]}

df_new = pd.DataFrame(data)

df_new.set_index(['category', 'post_code'], inplace=True)

# convert the index to a list of tuples and then to a dictionary
correlations = df_new.index.tolist()
correlations = {key: value for key, value in zip(correlations, df_new['corr'])}

# Step 4: Define decision variables
categories = df_agg_main_HP['DepartmentName'].unique()
post_groups = df_agg_main_HP['BranchName'].unique()
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
for post_group in post_groups:
    print(f"Post Group {post_group}:")
    for category in categories:
        print(f"    {category}: {allocation[(category, post_group)].varValue}")

# Create a dataframe to store the optimal allocation of retail categories to age groups
allocation_data = []
for post_group in post_groups:
    for category in categories:
        allocation_data.append({'Post Group': post_group, 'Category': category, 'Allocation': allocation[(category, post_group)].varValue})

allocation_df = pd.DataFrame(allocation_data)

# Reshape the dataframe
allocation_pivot = allocation        
allocation_df = pd.DataFrame(allocation_data)


# In[150]:


allocation_df.to_csv(r'C:\Users\roy_shaw\Desktop\allocation_loc_JM.csv')


# ## Approach Beta

# In[1]:


class Asset:
    pass


# In[2]:


Monthly_Margins_Count = 12


# In[5]:


directory = r"C:\Users\roy_shaw\Desktop\Work_Files\Campaign Analysis\Cleaned Data-OECS"
os.chdir(directory)
df_explore = pd.read_excel('margin_data.xlsx', sheet_name = 'JAM FY20-FY23')


# In[6]:


class Asset:
    def __init__(self, name: str, sales_history: pd.DataFrame):
        self.name = name
        self.expected_sales = np.mean(self.monthly_sales)
        self.total_sales = np.sum(self.monthly_sales)


# In[7]:


df_agg_main = pd.read_csv(r'C:\\Users\\roy_shaw\\Desktop\\Work_Files\\Campaign Analysis\\df_main_agg_JM.csv')


# In[8]:


df_dummy_input = df_agg_main[['Age Group', 'ItemCategory']]
#df_dummy_input_1 = df_agg_main[['AddressLine2', 'ItemCategory']]


# In[9]:


df_encoded = pd.get_dummies(df_dummy_input)
#df_encoded_1 = pd.get_dummies(df_dummy_input_1)


# In[10]:


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


# In[11]:


rows= []

for var1 in df_encoded:
    col = []
    for var2 in df_encoded :
        cramers =cramers_v(df_encoded[var1], df_encoded[var2]) # Cramer's V test
        col.append(round(cramers,2)) # Keeping of the rounded value of the Cramer's V  
    rows.append(col)
  
    cramers_results = np.array(rows)
df = pd.DataFrame(cramers_results, columns = df_encoded.columns, index =df_encoded.columns)


# Function to find correlation between two categorical column variables in a dataframe

# In[12]:


df_agg_main['ItemCategory'].unique()


# In[13]:


from functools import cache
from typing import List, Tuple
from scipy.optimize import minimize


# In[14]:


num_weights = 21
weights = np.random.random(num_weights)
weights /= np.sum(weights)


# In[15]:


print(list(weights))


# In[16]:


class Asset:
    pass

def get_log_period_margin(margin_history: pd.DataFrame):
    close = margin_history['Sales Revenue'].values 
    return np.log(close[1:] / close[:-1]).reshape(-1, 1)


# daily_price_history has to at least have a column, called 'Sales Revenue'

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

w_initial = list(weights)

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
        
    def optimize_sharpe_ratio(self):
        #Maximize Sharpe ratio = minimize minus Sharpe ratio
        res = minimize(
        lambda w: -(self._expected_return(w) - JAM_RF/100) / np.sqrt(self._variance(w)),
        random_weights(self.weights.size),
        constraints=[
            {'type': 'eq', 'fun': lambda w: np.sum(w) - 1.},
          ],
          bounds=[(0., 1.) for i in range(self.weights.size)]
        )

    
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


# In[17]:


def margins_retrieve_data(categories: List[str], df_values: pd.DataFrame):
    dataframes = []
    for cat_name in categories:
        val_margins = df_values[df_values['Category'] == cat_name]
        dataframes.append(val_margins)
    return dataframes


# In[18]:


stock_supply = ['Major White', 'Bedding', 'Business Equipment', 'Vision', 'Small Appliances', 'Accessories',                'Bedroom', 'Radio Shack', 'Occasional/Misc.', 'PCs & Notebooks', 'Lounge', 'Dining Room', 'Spare Parts',
               'Gaming', 'Home', 'Floor Coverings', 'Transport', 'Second Hand', 'Free Gift', 'Audio', 'Optical']

os.chdir('C:\\Users\\roy_shaw\\Desktop\\Work_Files\\Campaign Analysis\\Cleaned Data-OECS')
margins_dataframes = pd.read_excel('margin_data.xlsx', sheet_name = 'JAM FY20-FY23', header = 0)

split_margin_dataframes = margins_retrieve_data(stock_supply, margins_dataframes)

assets = tuple([Asset(name, margin_df) for name, margin_df in zip(stock_supply, split_margin_dataframes)])


# In[19]:


assets.__repr__()


# In[20]:


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

portfolio.optimize_with_expected_return(0.05)
plt.plot(np.sqrt(portfolio.variance), portfolio.expected_return, 'g+', markeredgewidth=5, markersize=20, label='optimize_with_expected_return(0.002)')

plt.xlabel('Portfolio standard deviation')
plt.ylabel('Portfolio expected (logarithmic) return')
plt.legend(loc='lower right')
plt.show()


# In[21]:


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


# ## Retail Stats to advance Unicomer

# In[ ]:


df_agg_main['']


# In[ ]:


groups = df_agg_main.groupby(['ItemCategory', 'AgreementTotal'], axis = 0)


# In[ ]:


groups['AgreementTotal'].sum()


# In[ ]:




