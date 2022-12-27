from datetime import timedelta

import pandas as pd
from nsepy import get_history


def getPrice(symbol, today, numdays):
    startdt = today - timedelta(days=numdays)
    data = pd.DataFrame()
    data = get_history(symbol=symbol, start=startdt, end=today)
    if (data.empty):
        print('Empty Price history')
        return data
    else:
        print("Price History of " + symbol + " from " + str(data.tail(numdays).index[0]) + " to expiry date: " + str(
            today))
        print(data.tail(numdays))
        # print(data.columns)
        # print(data["Close"])
        return data.tail(numdays)