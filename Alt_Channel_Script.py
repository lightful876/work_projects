#!/usr/bin/env python
# coding: utf-8

# Reporting for Revenues obtained by Ecommerce Sales

# Importing the required packages and uploading files into  a dataframe on pandas.
import pandas as pd
import os
from datetime import datetime


os.chdir('C:\\Users\\roy_shaw\\Desktop\\Work_Files\\Monthly\\FY24')
df_main = pd.read_csv('Jun_FY24_ecom_rev.csv')

#Creating a blank dataframe to copy elements to.
df_table = pd.DataFrame()

# Searching for Unique Countries in Report
lst_main_PP = list(df_main['Purchase Point'])
df_table['Country'] = list(set(lst_main_PP))

# Setting values for the reporting period
start_date_cur = datetime.strptime('01/06/2023','%d/%m/%Y')
end_date_cur = datetime.strptime('30/06/2023','%d/%m/%Y')

# Filtering the dataframe for the records that meet the criteria of being completed in the reporting period.
df_main[(df_main["Status"] == "Complete") & ((pd.to_datetime(df_main["Completed At"], dayfirst = True) >= start_date_cur) & (pd.to_datetime(df_main["Completed At"], dayfirst = True) <= end_date_cur))]

# Exploring each record in granularity to determine how much was spent according to the method used to make the purchase.
j = 0
cash_vals = []
PoD_vals = []
PIS_vals = []
SC_vals = []
cash_count_lst = []
PoD_count_lst = []
PIS_count_lst = []
SC_count_lst = []
lst_country = list(df_table['Country'])
large_country = list(df_main['Purchase Point'])
lst_status = list(df_main['Status'])
lst_PayMeth = list(df_main['Payment Method'])
lst_USD_total = list(df_main['USD Grand Total'])
lst_dates = pd.to_datetime(df_main['Completed At'], dayfirst = True)
x= set(lst_country)
while j in range(len(df_table['Country'])):
    val_sum = 0
    PoD_sum = 0
    PIS_sum = 0
    SC_sum = 0
    cash_count = 0
    PoD_count = 0
    PIS_count = 0
    SC_count = 0
    for i in range(len(df_main['ID'])):
        if((lst_status[i] == "Complete") & (lst_PayMeth[i] == "Credit/Debit Card") & (lst_country[j] == large_country[i]) & (lst_dates[i] >= start_date_cur) & (lst_dates[i] <= end_date_cur)):
            val_sum += lst_USD_total[i]
            cash_count += 1
        elif((lst_status[i] == "Complete") & (lst_PayMeth[i] == "Payment On Delivery") & (lst_country[j] == large_country[i]) & (lst_dates[i] >= start_date_cur) & (lst_dates[i] <= end_date_cur)):
            PoD_sum += lst_USD_total[i]
            PoD_count += 1
        elif((lst_status[i] == "Complete") & (lst_PayMeth[i] == "Pay In Store") & (lst_country[j] == large_country[i]) & (lst_dates[i] >= start_date_cur) & (lst_dates[i] <= end_date_cur)):
            PIS_sum += lst_USD_total[i]
            PIS_count += 1
        elif((lst_status[i] == "Complete") & ((lst_PayMeth[i] != "No Payment Information Required") | (lst_PayMeth[i] != "Gift Card")) & (lst_country[j] == large_country[i]) & (lst_dates[i] >= start_date_cur) & (lst_dates[i] <= end_date_cur)):
            SC_sum += lst_USD_total[i]
            SC_count += 1
    SC_vals.append(SC_sum)
    cash_vals.append(val_sum)
    PoD_vals.append(PoD_sum)
    PIS_vals.append(PIS_sum)
    cash_count_lst.append(cash_count)
    PoD_count_lst.append(PoD_count)
    PIS_count_lst.append(PIS_count)
    SC_count_lst.append(SC_count)
    j+=1
df_table['Cash Payments'] = cash_vals
df_table['PoD'] = PoD_vals
df_table['BOPIS'] = PIS_vals
df_table['Other'] = SC_vals

df_table['Cash Count'] = cash_count_lst
df_table['POD Count'] = PoD_count_lst
df_table['PIS_count'] = PIS_count_lst
df_table['SC_count'] = SC_count_lst

#Exporting to csv
df_table.to_csv(r'C:\\Users\\roy_shaw\\Desktop\\Work_Files\\week_12_Alt.csv')

