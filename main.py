import time

import pandas as pd
import pandas_datareader as dr
import matplotlib as plt
import yfinance as yf
import pymysql
from sqlalchemy import create_engine

# retrieve historical data with pandas_datareader
# pandas_datareader
# tickers = ['msft', 'aapl', 'twtr', 'intc']
#
# df1 = dr.DataReader(tickers, data_source='yahoo')
#
# df = dr.data.get_data_yahoo('IBM', start='2021-06-24', end='2021-07-26')
#
# df.info()
#
# df['Close'].plot(figsize=(15,5))
#
# plt.pyplot.show()

s_and_p_500_config = open('S_and_p_500_tickers_config.txt', 'r')

s_and_p_500 = s_and_p_500_config.readlines()

engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                       .format(user="root",
                               pw="******",
                               db="s_and_p_500"))

for ticker in s_and_p_500:
    print(ticker.strip().lower())

    # Underlying Open, Close, High, Low, Volume (5 Minute Intervals).
    # Populates tables: aapl, aal, amzn, etc.
    try:
        data = yf.download(tickers=ticker.strip(), period='1d', interval='5m')

        data.to_sql(ticker.strip().lower(), con=engine, if_exists='append', chunksize=1000)

        time.sleep(1.1)

    except Exception as e:
        print(ticker.strip() + " failed.")

    # Underlying Daily Total Volume
    # Populates table: undl_daily_total_vol

    # Underlying Last Earnings Date
    # Populates table: undl_last_earnings
