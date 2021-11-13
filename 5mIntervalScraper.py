import logging
import time
import yfinance as yf
import pymysql
from sqlalchemy import create_engine
from datetime import datetime

closedDates = open('C:/Users/tsher/Desktop/MarketMaker/MarketClosedDates.txt', 'r')

closedDatesArray = []

for date in closedDates:
    closedDatesArray.append(date.strip())

if datetime.today().strftime('%Y-%m-%d') not in closedDatesArray:
    # Begin scraping
    logName = datetime.today().strftime('C:/Users/tsher/Desktop/MarketMaker/5mIntervalScraper/log/%Y-%m-%d_5minute.log')

    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    logging.basicConfig(filename=logName, level=logging.INFO)

    s_and_p_500_config = open('C:/Users/tsher/Desktop/MarketMaker/s_and_p_500_tickers_config.txt', 'r')

    s_and_p_500 = s_and_p_500_config.readlines()

    s_and_p_500_config.close()

    engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                           .format(user="root",
                                   pw="*****",
                                   db="s_and_p_500"))

    daysToCollect = 1
    daysToCollectString = "1d"

    for ticker in s_and_p_500:
        # Underlying Open, Close, High, Low, Volume (5 Minute Intervals).
        # Populates tables: aapl, aal, amzn, etc.
        try:
            data = yf.download(tickers=ticker.strip(), period=daysToCollectString, interval='5m')

            tableName = ticker.strip().lower()

            data.to_sql(tableName, con=engine, if_exists='append', chunksize=1000)

            logging.info(ticker.strip() + " SUCCESS")

            time.sleep(daysToCollect)

        except Exception as e:
            logging.error(ticker.strip() + " FAILURE")
            logging.error(e)

    logging.shutdown()