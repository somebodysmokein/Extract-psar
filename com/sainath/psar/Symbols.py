from nsepy.symbols import get_symbol_list
import pandas as pd


class Symbols:
    __finalList = pd.DataFrame(columns=['Symbol', 'Close', 'sma', 'ema'])

    def get_symobls(self):
        return get_symbol_list()

    def extract_data_to_csv(self):
        self.__finalList.to_csv('data.csv')

    def add_to_list(self, symbol):
        print("Symbol data below")
        print(symbol)
        print("Existing data frame")
        print(self.__finalList)
        print("index of data frame =>" + str(len(self.__finalList.index)))
        # self.__finalList.loc[len(self.__finalList.index)] = symbol
        # self.__finalList.append(symbol, ignore_index=True)
        self.__finalList=pd.concat([self.__finalList, symbol])
        print("New Dataframe")
        print(self.__finalList)