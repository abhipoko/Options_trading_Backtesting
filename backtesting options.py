
import datetime as dt
from credentials import api_key,secret_key
import pickle
import sys
from zoneinfo import ZoneInfo
from alpaca.data.live.option import *
from alpaca.data.live.stock import *
from alpaca.data.historical.option import *
from alpaca.data.historical.stock import *
from alpaca.data.requests import *
from alpaca.data.timeframe import *
from alpaca.trading.client import *
from alpaca.trading.stream import *
from alpaca.trading.requests import *
from alpaca.trading.enums import *
from alpaca.common.exceptions import APIError
from alpaca.data.models import OptionsSnapshot
from datetime import datetime, timedelta
from calendar import monthrange, weekday, WEDNESDAY, THURSDAY
import pandas as pd
import time as t
import sqlite3
import logging

pd.options.mode.chained_assignment = None  # default='warn'
logging.basicConfig(filename='option_backtesting.log',filemode='w',level=logging.INFO, format='%(message)s')
logging.info('this is my first line')



data_adress='/Users/abhisheksonawane/Desktop/python prg/fessorpro/template/option_history.db'
holidays = ['2021-01-26', '2021-03-11', '2021-03-29', '2021-04-02', '2021-04-14', '2021-04-21', '2021-05-13', '2021-07-21', '2021-08-19', '2021-09-10', '2021-10-15', '2021-11-04', '2021-11-05', '2021-11-19', '2022-01-26', '2022-03-01', '2022-03-18', '2022-04-14', '2022-04-15', '2022-05-03', '2022-08-09', '2022-08-15', '2022-08-31', '2022-10-05', '2022-10-24', '2022-10-26', '2022-11-08', '2023-01-26', '2023-03-07', '2023-03-30', '2023-04-04', '2023-04-07', '2023-04-14', '2023-04-21', '2023-05-01', '2023-06-28', '2023-08-15', '2023-09-19', '2023-10-02', '2023-10-24', '2023-11-14', '2023-11-27', '2023-12-25']
holidays = [datetime.strptime(x,'%Y-%m-%d')for x in holidays] # Converting holiday strings to datetime objects
# print(holidays) 




def get_weekly_expiry(year, month):
    d=monthrange(year, month)[1]
    thursdays=[datetime(year, month, day) for day in range(1,d+1) if datetime(year,month,day).weekday()==3]
    for hol in holidays:
        if hol in thursdays:
            thursdays[thursdays.index(hol)]=hol-timedelta(days=1) # update thursday to wednesday incase of thursday holiday
    return thursdays
l=get_weekly_expiry(2023,1)
print(l)




def get_nearest_expiry(current_day=datetime.now):
    year=current_day.year
    month=current_day.month
    last_day_of_month=monthrange(year, month)[1]
    thursdays=[datetime(year, month,x) for x in range(1,last_day_of_month+1) if  datetime(year,month,x).weekday()==3]
    for hol in holidays:
        if hol in thursdays:
            thursdays[thursdays.index[hol]]=hol-timedelta(days=1)
    
    current_expiry=None
    for curr_thursdays in thursdays:
        if current_day <= curr_thursdays:
            current_expiry =curr_thursdays
            break
    # handle the case if today is after last thursday of the month
    if current_day>thursdays[-1]:
            if month==12:
                #move to next year and set month to january
                year+=1
                month=1
            
            else:
                month+=1
        #calculate thursdays for next month
            d=monthrange(year, month)[1]
            thursdays=[datetime(year, month, day) for day in range (1,d+1) if weekday(year, month, day)==THURSDAY]
        # adjust for holidays in next month
            for hol in holidays:
                if hol in thursdays:
                    thursdays[thursdays.index(hol)]=hol - timedelta(days=1)
            current_expiry=thursdays[0]
            return current_expiry
    return current_expiry 



def get_from_database():
    con=sqlite3.connect(data_adress)
    cursorObj=con.cursor()
    cursorObj.execute('SELECT name from sqlite_master where type- "table" ')
    data=cursorObj.fetchall()
    option_price_df={}
    temp=0
    for i in data:
        k=i[0]
        option_price_df[k]=pd.read_sql_query(f'SELECT * FROM {k}', con)
    return option_price_df



year=2023
month=1
money=2000
trades=open('trades.csv','w')
trades.write('time'+","+'option_contract_name' +","+'position'+','+'option_price'+','+'underlying_price'+','+'balance'+'\n')
option_price_df1=get_from_database()
option_price_df={}
#resample 1min to 5min
for i,j in option_price_df1.items():
    if j.empty == False:
        j=j[['datetime','open','high','low','close','volume']]
        j['datetime']=pd.to_datetime(j['datetime'])
        j.set_index('datetime', inplace=True)
        ohlcv_dict={
            'open':'first',
            'high':'max',
            'low':'min',
            'close':'last',
            'volume':'sum'

        }
    option_price_df[i]=j.resample('5min').agg(ohlcv_dict)
    option_price_df[i].dropna(inplace=True)
    option_price_df[i]=option_price_df[i].between_time('09:15','15:30')
    option_price_df[i].reset_index(inplace=True)
    for month in range(1,7):
        end=get_weekly_expiry(year,month)[-1]
        start=datetime(year,month,1)
        underlying_df_daily=option_price_df['daily'+end.strftime('%Y%m%d')]
        underlying_df_5min=option_price_df['min'+end.strftime('%Y%m%d')]
        for i in underlying_df_daily.index:
            open_price=(int(float(underlying_df_daily['open'][i]))//100)*100
            time=underlying_df_daily['datetime'][i]
            if time>datetime(2023,6,20):
                break
                
            start=datetime(time.year,time.month,time.day,9,15)
            end2=datetime(time.year,time.month,time.day,15,25)
            portfolio={}
            first_trade=False
            while start<=end2:
                try:
                    end=get_nearest_expiry(time).strftime('%Y%m%d')
                    atm='call'+str(open_price)+end
                    spot_price=underlying_df_5min[underlying_df_5min['datetime']==start.strftime('%Y-%m-%d %H:%M:%S')].open.values[0]
                    
                except  Exception as e:
                    logging.info('eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'+str(atm)+','+str(start)+','+str(end))
                    start=start+timedelta(days=2)
                    continue
