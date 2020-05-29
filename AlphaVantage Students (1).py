#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#Enter your wcd here if needed


# ### Install Packages

# The first segment will be about installing all necessary packages to run alphavantage as well as important files that you will need througout this script

# In[ ]:


pip install alpha_vantage


# In[ ]:


from alpha_vantage.timeseries import TimeSeries
from alpha_vantage.techindicators import TechIndicators
from matplotlib.pyplot import figure
import matplotlib.pyplot as plt
import pandas as pd
import pandas
import numpy as np
import sys
import random
import time
from time import sleep
from scipy import interpolate
import math


# Remember, the more data you want to download the more keys you will need for each 500 stocks you will need one additional free key. 

# In[1]:


key_list = ['Enter your api keys here']


# You need yo use a specific column header to concat the Alphavantage output with what it requested.
# The format should be 'YYYY-MM-DD HH:MM:SS AM/PM' , this can be a bit tideous but should only take about 5 minutes a week. This seems like a unnecessary step, but not every stock is traded every minute resulting in gaps in the data that will be filled later on.

# In[ ]:


columns = pd.read_csv('Columns.csv')
cc = columns.columns
cc = cc.drop('date')  #this is for the columns.csv file we used, but if your format is different feel free to change it


# In[ ]:


cc = pd.to_datetime(cc) #transform the format of the string into a datetime, this will be used in retrieving the data


# In[ ]:


tickers_list = pd.read_csv("companylist.csv") # This should be your ticker list you want to check
comp_list = tickers_list.iloc[:,0]


# ### Check Ticker List

# This function allows the user to submit a csv file that contains a list of tickers. Alphavnatage does not publish a list of all supported tickers, and therefore I will publish one as they might have their reason. However, this tool will need a time due to the limimtations of alphanatage of 5 calls per minute, but will filter all 'bad' tickers out so that the outcome will be a clean list of functioning tickers. I would repeat this step every time you pull large data sets for all tickers, as tickers change, are discontinued etc. 

# In[ ]:


comp_list = list(np.zeros(0))

def data_gath(i,tickers_list,key_list):  
    sleep(12) 
    ticker = tickers_list.iloc[i,0]
    j = i//500
    key = key_list[j]
    time = TimeSeries(key=key, output_format='pandas')
    data = time.get_intraday(symbol=ticker, interval = '1min',outputsize = 'full')
        
    
for i in range(len(tickers_list)):
    try:
        data_gath(i,tickers_list,key_list)
        comp_list.append(tickers_list.iloc[i,0])
    except:
        print(str(i))


# In[ ]:


comp_list_pd = pd.DataFrame(comp_list)
comp_list_pd.to_csv("Full_list.csv")


# ### Retrieving Data

# Now that we have a list of tickers we can download up to 5 days of intraday data and other historical data. The following section retrieves the stock data and puts them into a df that is easily readable. At the end we merge the indivual ticker blocks into one df that you should download as a 'raw' data file.

# Weekly data pull

# In[ ]:


def Stock_pull(df,key_list,columns_1):
    
    df_new = pd.DataFrame(np.zeros([0, len(columns_1)]))
    df_new.columns = columns_1
    df_new['Ticker'] = 0
    
    #for i in range(len(df)):
    for i in range(2):
        time_remaining = (((len(df)-i)*12)/60) 
        print (str(time_remaining) +" Minutes remaining")
        
        sleep(12) #limit of 5 pulls per minute and 500 per day
        ticker = df[i]
        j = i//500
        key = key_list[j]
        time = TimeSeries(key=key, output_format='pandas')
        data = time.get_intraday(symbol=ticker, interval = '1min',outputsize = 'full')
        
        df_int = data[0].T  # new df with length of 390 * days        
        df_int.iloc[3,:] = df_int.iloc[1,:] - df_int.iloc[2,:]
        df_int = df_int.rename(index={'4. close': '4. HML'})
        df_int['Ticker'] = df[i]
        
        df_new = pd.concat([df_new, df_int], axis=0)
    
    return df_new


# In[ ]:


WK = Stock_pull(comp_list,key_list,cc)


# In[ ]:


WK.to_csv("WK1_df.csv") # save this Raw data file each week as we will later merge 4 weeks into one big file


# ### Data Merging + Cleaning

# The raw data is not perfect and sometimes is missing data, while this could be for a number of reasons we will 'clean' the data, interpolate values and adjust missing volumes. 

# In[ ]:


WK1_df = pd.read_csv("WK1_df.csv") #These are 4 weeks of data that we downloaded and now we will merge them as well as
WK2_df = pd.read_csv("WK2_df.csv") #clean them to ensure we have accurate data
WK3_df = pd.read_csv("WK3_df.csv")
WK4_df = pd.read_csv("WK4_df.csv")
df_list = [WK1_df,WK2_df,WK3_df,WK4_df]


# In[ ]:


def Merge(df_list):
    df_list[len(df_list)-1]
    df = pd.DataFrame()
    
    for i in range(len(df_list)):
        if i == 0:
            df_list[0] = df_list[0].iloc[:, :-1]
            df = df_list[0]
        if i > 0 and i < (len(df_list)-1):
            df_list[i] = df_list[i].iloc[:, 1:-1]
            df = pd.concat([df, df_list[i]], axis=1)
        if i == (len(df_list)-1):
            df_list[i] = df_list[i].iloc[:, 1:]
            df = pd.concat([df, df_list[i]], axis=1)    
    return df


# In[ ]:


WK1_4 = Merge(df_list)   # Merged file that consists of all 4 weeks


# In[ ]:


def cleaning_arr(pd_df):
    
    col = pd_df.columns
    df = np.array(pd_df)
    
    for i in range(len(df)):  
        print('Reihe ' + str(i))
        
        n=1
        
        if df[i,0] == '1. open':
            for j in range(len(df[i,:].T)):
                if pd.isna(df[i,j]) == True:
                    if pd.isna(df[i,j-1]) == False:
                        df[i,j] = df[i,j-1]     
                    if type(df[i,j-1]) == str:
                        while True:
                            n += 1
                            df[i,1] = df[i,j+n-1]
                            if pd.isna(df[i,j+n-1]) == False:
                                break
                                
        if df[i,0] == '2. high':
            for j in range(len(df[i,:].T)):
                if pd.isna(df[i,j]) == True:
                    df[i,j] = df[i-1,j]
                    
        if df[i,0] == '3. low':
            for j in range(len(df[i,:].T)):
                if pd.isna(df[i,j]) == True:
                    df[i,j] = df[i-2,j]
                    
        if df[i,0] == '4. HML':
            for j in range(len(df[i,:].T)):
                if pd.isna(df[i,j]) == True:
                    df[i,j] = df[i-2,j]-df[i-1,j] 
                    
        if df[i,0] == '5. volume':
            for j in range(len(df[i,:].T)):
                if pd.isna(df[i,j]) == True:
                    df[i,j] = 0           
          
        
    df = pd.DataFrame(df)
    df.columns = pd_df.columns
    df.index = df.iloc[:,0]
    df = df.drop(df.columns[0], axis=1)
    
    return df


# In[ ]:


WK1_4_clean = cleaning_arr(WK1_4)  #this calls up the previous function and cleans it 


# In[ ]:


WK1_4_clean.to_csv('WK1_4_clean.csv')


# ### Data Analysis

# In[ ]:


def Analysis(pd_df):
    
    pd_df['Max'] = 0
    pd_df['Min'] = 0
    pd_df['Mean'] = 0
    pd_df['Median'] = 0
    pd_df['StD'] = 0
    pd_df['Max Minute Change'] = 0
    pd_df['Min Minute Change'] = 0
    
    col = pd_df.columns
    df = np.array(pd_df)
    values = [0]*len(df[1,1:-8])

    
    for i in range(len(df)):  
    
        print('Reihe ' + str(i))
        
        df[i,-7] = df[i,1:-8].max()
        df[i,-6] = df[i,1:-8].min() 
        df[i,-5] = df[i,1:-8].mean()
        df[i,-4] = np.median(df[i,1:-8]) 
        df[i,-3] = df[i,1:-8].std() 
        
        for j in range(len(df[1,1:-8])):
            if j > 1:
                if df[i,j] != 0 and df[i,j-1] != 0:
                    values[j] = (df[i,j] / df[i,j-1])-1

                if df[i,j] == 0 or df[i,j-1] == 0:
                    values[j] = 0
            
        df[i,-2] = max(values) 
        df[i,-1] = min(values) 
         
        
    df = pd.DataFrame(df)
    df.columns = pd_df.columns
    df.index = df.iloc[:,0]
    df = df.drop(df.columns[0], axis=1)
    
    return df

