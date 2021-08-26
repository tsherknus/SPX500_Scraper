import time
import pandas as pd
import pandas_datareader as dr
import matplotlib as plt
import yfinance as yf
import pymysql
from sqlalchemy import create_engine

s_and_p_500_config = open('S_and_p_500_tickers_config.txt', 'r')

s_and_p_500 = s_and_p_500_config.readlines()

engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                       .format(user="",
                               pw="",
                               db=""))

for ticker in s_and_p_500:
    print(ticker.strip().lower())

    # Underlying Open, Close, High, Low, Volume (5 Minute Intervals).
    # Populates tables: aapl, aal, amzn, etc.
    try:
        data = yf.download(tickers=ticker.strip(), period='1d', interval='5m')

        data.to_sql(ticker.strip().lower(), con=engine, if_exists='append', chunksize=1000)

        time.sleep(1.1)

    except Exception as e:
        print(ticker.strip() + " 5m failed.")

    # Underlying Daily Total Volume
    # Populates table: undl_daily_total_vol
    try:
        data = yf.download(tickers=ticker.strip(), period='1d', interval='1d')

        tableName = ticker.strip().lower() + '_daily'

        data.to_sql(tableName, con=engine, if_exists='append', chunksize=1000)

        time.sleep(1.1)

    except Exception as e:
        print(ticker.strip() + " 1d failed.")


    # Underlying Last Earnings Date
    # Populates table: undl_last_earnings
