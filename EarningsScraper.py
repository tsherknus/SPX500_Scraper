import time
import pandas as pd
import pandas_datareader as dr
import matplotlib as plt
import yfinance as yf
import pymysql
from sqlalchemy import create_engine
from datetime import datetime
from datetime import timedelta
from yahoo_earnings_calendar import YahooEarningsCalendar
import dateutil.parser

import requests
import json

s_and_p_500_config = open('S_and_p_500_tickers_config.txt', 'r')

s_and_p_500 = s_and_p_500_config.readlines()

engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                       .format(user="root",
                               pw="*****",
                               db="s_and_p_500"))

for ticker in s_and_p_500:
    print(ticker.strip().lower())

    #Underlying Last Earnings Date
    #Populates table: undl_last_earnings
    try:
        url = 'https://finance.yahoo.com/calendar/earnings?symbol=' + ticker.strip()
        time.sleep(10)
        page = requests.get(url, headers={'User-Agent': 'Custom'})
        page_content = page.content.decode(encoding='utf-8', errors='strict')
        page_data_string = [row for row in page_content.split(
            '\n') if row.startswith('root.App.main = ')][0][:-1]
        page_data_string = page_data_string.split('root.App.main = ', 1)[1]

        page_data_dict = json.loads(page_data_string)

        page_data_dict = page_data_dict["context"]["dispatcher"]["stores"]["ScreenerResultsStore"]["results"]["rows"]

        output = pd.DataFrame.from_dict(page_data_dict)

        tableName = ticker.strip().lower() + '_earnings'

        output.to_sql(tableName, con=engine, if_exists='append', chunksize=1000)

    except Exception as e:
        print(ticker.strip() + " future earnings failed.")

    #Get earnings over a set period https://towardsdatascience.com/how-to-download-the-public-companies-earnings-calendar-with-python-9241dd3d15d
    DAYS_AHEAD = 180

    # setting the dates
    start_date = datetime.now().date()
    end_date = (datetime.now().date() + timedelta(days=DAYS_AHEAD))
    earnings_df = output

    # extracting the date from the string and filtering for the period of interest
    earnings_df['report_date'] = earnings_df['startdatetime'].apply(lambda x: dateutil.parser.isoparse(x).date())
    earnings_df = earnings_df.loc[earnings_df['report_date'].between(start_date, end_date)] \
                             .sort_values('report_date')