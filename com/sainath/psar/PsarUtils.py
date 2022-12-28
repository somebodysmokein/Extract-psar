from datetime import date

import pandas as pd
import talib
from matplotlib import pyplot as plt
from nsepy import get_history
from pandas import DataFrame


class PsarUtils:
    __stocks = pd.DataFrame(
        columns=['Symbol', 'Series', 'Prev Close', 'Open', 'High', 'Low', 'Last', 'Close', 'VWAP', 'Volume', 'Turnover',
                 'Trades', 'Deliverable Volume', '%Deliverble', 'SAR'],
        index=['Date'])
    __crossOver=DataFrame()

    def __init__(self):
        print("Calling PsarUtils")

    def getStocks(self):
        return self.__stocks

    def getPsar(self, stocks):
        # data = get_history(symbol="SBIN", start=date(2022, 12, 1), end=date(2022, 12, 26))
        print(stocks)
        data = stocks.dropna()
        data.tail()
        data.tail().to_csv('data.csv')
        data['SAR'] = talib.SAR(data.High, data.Low, acceleration=0.02, maximum=0.2)
        # self.__stocks=data
        return data

    def getCrossOver(self,stocks) -> DataFrame:
        print(stocks)
        print(stocks.tail(1))
        inital_value=0
        stocks=stocks.iloc[::-1]
        counter = 1
        for item in stocks.index:
            print("Index => "+ str(item))
            print(stocks['SAR'][item])
            if stocks['SAR'][item].item() > stocks['Close'][item].item():
                print("Last sell price => "+ str(stocks['Close'][item]))
                #stocks.insert()
                stocks['Signal']="Sell"
                sell_signal=1
            else:
                stocks['Signal'] = "Buy"
                sell_signal=-1
            if counter==1:
                inital_value=sell_signal
            print("init value => "+str(inital_value)+"; Sell Signal =>"+str(+sell_signal))
            if sell_signal != inital_value:
                print(item)
                print("Sell Signal & initial value are not equal")
                print(stocks['SAR'][item])
                self.__crossOver=stocks.loc[[item]]
                return self.__crossOver
            counter += 1
            #print("Stock columns => "+stocks.columns)
            prevItem=item
        return pd.DataFrame()

    def filterBasedOnCloseValue(self, stocks):
        counter=0
        print("Calling filterBasedOnCloseValue with "+str(self.__crossOver['SAR'].item()))
        print(stocks.tail(3))
        for item in stocks.tail(3).index:
            print("Counter value => "+ str(counter))
            if stocks['Close'][item].item() > self.__crossOver['SAR'].item():
                 counter=counter+1
        if counter > 2:
            print("CrossOver SAR => " + str(self.__crossOver['SAR']) + "; Close Price => " + str(
                stocks.Close.item()) +
                  ", Since the Cut off not met, we don't enter trade")
            emptyFrame = pd.DataFrame()
            return emptyFrame
        else:
            if stocks['Close'].tail(1).item() > self.__crossOver['SAR'].item():
                print("CrossOver SAR => " + str(self.__crossOver['SAR']) + "; Close Price => " + str(
                    stocks.Close.tail(1).item()) +
                      ", So we enter trade")
                self.__stocks = pd.concat([self.__stocks, stocks.tail(1)])
                return self.__stocks
            else:
                emptyFrame = pd.DataFrame()
                return emptyFrame

        # else:
        #     print("SAR value of " + str(self.__stocks.Symbol.tail(1)) + " is not greater than Close Price")
