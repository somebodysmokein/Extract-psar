from datetime import date, timedelta
import pandas as pd
import math

from com.sainath.price.PriceUtils import getPrice


class EmaUtils:

    def extractcloseData(self,df):
        print("Closing data for last 20 days ")
        print(df['Close'])

    def calculateMVA(self,df):
        mva = df["Close"].mean()
        # print("Simple Moving average: "+str(mva))
        return mva

    def calculateEMA(self,symbol, df, numdays):
        lastRow = df.tail(1)
        print("lastRow => "+ str(lastRow))
        priordf = getPrice(symbol=symbol, today=df.index[0] - timedelta(days=1)
                           , numdays=numdays)
        prevEMA = self.calculateMVA(priordf)
        finalEMA = 0
        for ind in df.index:
            currentEMA = self.emaforDate(prevEMA, df['Close'][ind], numdays)
            print("Current EMA " + str(currentEMA))
            if math.isnan(currentEMA):
                return None
            else:
                # print("EMA for "+str(ind)+"  "+str(currentEMA))
                # cummulativeEMA=prevEMA+currentEMA
                prevEMA = currentEMA
                finalEMA = currentEMA
        print("Final EMA :" + str(finalEMA))
        return finalEMA

    def emaforDate(self, emayesterDay, today, numdays):
        multiplier = 2 / (numdays + 1)
        print("Multiplier => " + str(multiplier))
        emaToday = today * multiplier + emayesterDay * (1 - multiplier)
        print("EMA Today => " + str(emaToday))
        return emaToday