from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, date
from nsepy.symbols import get_symbol_list
import pandas as pd

from com.sainath.ema.EmaUtils import EmaUtils
from com.sainath.price.PriceUtils import getPrice
from com.sainath.psar.PsarUtils import PsarUtils
from com.sainath.psar.Symbols import Symbols

symbolData = Symbols()
symboldf = get_symbol_list()
print(symboldf)


def psarSignal(symbol):
    print("Symbol => " + str(datetime.now().date()))
    date_object = datetime.strptime(str(datetime.now().date()), '%Y-%m-%d').date()
    data = getPrice(symbol=symbol, today=date_object, numdays=90)
    print(data)
    # Plot the closing price
    if not data.empty:
        psarObj = PsarUtils()
        data = psarObj.getPsar(stocks=data)
        data.to_csv('SAR-full.csv')
        cross_over_data = psarObj.getCrossOver(stocks=data)
        if not cross_over_data.empty:
            if not data.empty:
                psar_based_stock_list = psarObj.filterBasedOnCloseValue(stocks=data)
                if not psar_based_stock_list.empty:
                    psar_based_stock_list = psar_based_stock_list.dropna()
                    print("Filtered Stocks => " + str(psar_based_stock_list))
                    if psar_based_stock_list['Close'].item() > psar_based_stock_list['SAR'].item():
                        psar_based_stock_list['Signal'] = "Buy"
                    else:
                        psar_based_stock_list['Signal'] = "Sell"
                    return psar_based_stock_list
                else:
                    print("DataFrame empty after filter")
                    return pd.DataFrame()
            else:
                return pd.DataFrame()
        else:
            print("No Cross Over Data")
            return pd.DataFrame()
    else:
        print("No Price Data")
        return pd.DataFrame()

    # return psar_based_stock_list
    # print("------Final List-------")
    # print(psar_based_stock_list)

    # data.SAR.plot(figsize=(10,5))
    # plt.grid()
    # plt.show()
    # Calculate parabolic sar
    # data['SAR'] = talib.SAR(data.High, data.Low, acceleration=0.02, maximum=0.2)
    # data.tail().to_csv('Sar_data.csv')
    # Plot Parabolic SAR with close price
    # data[['Close', 'SAR']][:500].plot(figsize=(10,5))
    # plt.grid()
    # plt.show()
    # data[['Close','SAR']].tail().to_csv('Data-SAR.csv')


stockList = pd.DataFrame(
    columns=['Symbol', 'Series', 'Prev Close', 'Open', 'High', 'Low', 'Last', 'Close', 'VWAP', 'Volume', 'Turnover',
             'Trades', 'Deliverable Volume', '%Deliverble', 'SAR', 'Signal'],
    index=['Date'])

emaExtractedRecords = pd.DataFrame(
    columns=['Symbol', 'Series', 'Prev Close', 'Open', 'High', 'Low', 'Last', 'Close', 'VWAP', 'Volume', 'Turnover',
             'Trades', 'Deliverable Volume', '%Deliverble', 'SAR', 'Signal'],
    index=['Date'])
symbols = []
counter = 0


def asyncTask(symbolforExtraction):
    global stockList
    lastestData = psarSignal(symbol=symbolforExtraction)
    if not lastestData.empty:
        stockList = pd.concat([stockList, lastestData])


def emaExtraction(symbol):
    global psarFilteredStocks, emaExtractedRecords
    tempdf = psarFilteredStocks[psarFilteredStocks.Symbol == symbol]
    if not tempdf.empty:
        print("Filtered based on symbol : " + str(tempdf))
        emaUtils = EmaUtils()
        ema200 = emaUtils.calculateEMA(symbol=symbol, df=tempdf, numdays=200)
        ema9 = emaUtils.calculateEMA(symbol=symbol, df=tempdf, numdays=9)
        ema21 = emaUtils.calculateEMA(symbol=symbol, df=tempdf, numdays=21)
        print("Results => Close Price: " + str(tempdf['Close'].item()) + "; 9EMA: " + str(ema9) + "; 21EMA: " + str(
            ema21) + "; 200EMA: " + str(ema200))
        if tempdf.Close.item() > ema9 > ema21 > ema200:
            print("EMA Filter satisfied: Close Price > ema9 > ema21 > ema200")
            tempdf['ema9'] = ema9
            tempdf['ema21'] = ema21
            tempdf['ema200'] = ema200
            emaExtractedRecords = pd.concat([emaExtractedRecords, tempdf])
        else:
            print("EMA Filter not satisfied")
    else:
        print("Symbol " + symbol + " not found!!")


if __name__ == '__main__':
    for ind in symboldf.index:
        symbolforExtraction = symboldf['SYMBOL'][ind]
        print("type(symbolforExtraction) =>" + str(type(symbolforExtraction)))
        symbol = symbolforExtraction
        if symbol == 'DVL':
            print('Skip DVL stock')
        else:
            print(symbolforExtraction)
            symbols.insert(counter, symbolforExtraction)
            counter = counter + 1

    with ThreadPoolExecutor(max_workers=30) as exe:
        exe.map(asyncTask, symbols)
    stockList = stockList.dropna()
    print("Stokclist => " + str(stockList))
    stockList.loc[stockList['Signal'] == 'Buy'].to_csv('finalized-list.csv')
    psarFilteredStocks = stockList.loc[stockList['Signal'] == 'Buy']

    #emaExtraction("63MOONS")
    with ThreadPoolExecutor(max_workers=30) as exe:
        exe.map(emaExtraction, psarFilteredStocks['Symbol'])
    print("Final list of stocks after Ema Extraction => " + str(emaExtractedRecords.dropna()))
    emaExtractedRecords.dropna().to_csv('finalized-list.csv')
