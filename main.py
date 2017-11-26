import subprocess
import os
import matplotlib.pyplot as plt
from matplotlib import style
import bs4 as bs
import numpy as np
import pickle
import requests
import datetime as dt
import pandas as pd
import pandas_datareader.data as web
import fix_yahoo_finance

style.use("ggplot")
fix_yahoo_finance.pdr_override()

def save_tickers(url='https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'):
    resp = requests.get(url)
    soup = bs.BeautifulSoup(resp.text, 'lxml')
    table = soup.find('table', {'class':'wikitable sortable'})
    tickers = []
    for row in table.findAll('tr')[1:]:
        ticker = row.findAll('td')[0].text
        tickers.append(ticker)

    with open("tickers.pickle", "wb") as f:
        pickle.dump(tickers, f)

    print(tickers)
    return tickers

def get_data_from_yahoo(reload_tickers=False):
    if reload_tickers:
        tickers = save_tickers()
    else:
        with open("tickers.pickle", "rb") as f:
            tickers = pickle.load(f)

    if not os.path.exists("stock_data"):
        os.makedirs('stock_data')

    start = dt.datetime(2000,1,1)
    end = dt.datetime(2017,11,21)

    for ticker in tickers:
        if os.path.exists("stock_data/{}.csv".format(ticker)) and os.path.getsize("stock_data/{}.csv".format(ticker)) < 5:
            print("\n Removing {}, because previous creation failed due to throttling".format(ticker))
            os.remove("stock_data/{}.csv".format(ticker))
        if not os.path.exists("stock_data/{}.csv".format(ticker)):
            df = web.get_data_yahoo(ticker, start, end)
            df.to_csv("stock_data/{}.csv".format(ticker))
        else:
            print ("Already have {}".format(ticker))

def compile_data():
    with open("tickers.pickle", "rb") as f:
        tickers = pickle.load(f)

    main_df = pd.DataFrame()

    for count, ticker in enumerate(tickers):
        df = pd.read_csv("stock_data/{}.csv".format(ticker))
        df.set_index('Date', inplace = True)
        df.rename(columns = {"Adj Close": ticker}, inplace=True)
        df.drop(["Open", "High", "Low", "Close", "Volume"], 1, inplace=True)

        if main_df.empty:
            main_df = df
        else:
            main_df = main_df.join(df, how="outer")

        if count % 10 == 0:
            print(count)

    print(main_df.head())
    main_df.to_csv("sp500_joined_closes.csv")


get_data_from_yahoo()
compile_data()
