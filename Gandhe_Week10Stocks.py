#Author: Sreeja Gandhe
#21 Aug 2022
#IMPROVING THE STOCK PROBLEM WITH ADDITIONAL FUNCTIONALITY

import pandas as pd
import sqlite3
import json
import plotly.express as px
from datetime import datetime

# Database Connection
database_Path = r'C:\Users\Sreej\Documents\Quarter3_Summer\Python Programming\Week10\Stocks_json.db'
conn = sqlite3.connect(database_Path)
conn_cursor = conn.cursor()

# Dataset using pandas
f_path = r'C:\Users\Sreej\Documents\Quarter3_Summer\Python Programming\Week10\AllStocks.json'
with open(f_path) as json_file:
    dataset = json.load(json_file)


class createData:
    def __init__(self, symbol, date, value):
        self.symbol = symbol
        self.date = date
        self.value = value

    def createTable(self):
        create_table = " CREATE TABLE IF NOT EXISTS " + self.symbol
        create_table = create_table + "( Date date PRIMARY KEY," + self.symbol + " real NOT NULL ) "
        conn_cursor.execute(create_table)

    def insertData(self):
        insertData = 'INSERT or IGNORE INTO ' + self.symbol + " VALUES ('"
        insertData = insertData + str(self.date)
        insertData = insertData + " ',' " + str(self.value) + "' );"
        conn_cursor.execute(insertData)


symbols = ['GOOG', 'AIG', 'MSFT', 'RDSA', 'FB', 'M', 'F', 'IBM']
stockDict = {'GOOG': 25, 'AIG': 235, 'MSFT': 85, 'RDS-A': 400, 'FB': 130, 'M': 425, 'F': 85, 'IBM': 80}

for data in dataset:
    try:
        s_date = datetime.strptime(data['Date'], '%d-%b-%y')
    except ValueError:
        print('Update date format')

    if data['Symbol'] == 'RDS-A':
        stock_value = round(data['Close'] * stockDict['RDS-A'], 2)
        newStock = createData('RDSA', s_date, stock_value)
        newStock.createTable()
        newStock.insertData()
    else:
        stock_value = round(data['Close'] * stockDict[data['Symbol']], 2)
        newStock = createData(data['Symbol'], s_date, stock_value)
        newStock.createTable()
        newStock.insertData()

# Merge to data frame
select_dates = "SELECT Date FROM AIG"
df_all_stocks = pd.read_sql_query(select_dates, conn)
for stock in symbols:
    select_table = "SELECT * from " + stock
    df_name = stock + '_df'
    df_name = pd.read_sql_query(select_table, conn)
    df_all_stocks = pd.merge(df_all_stocks, df_name, how='left', on=["Date"])

# Plot the graph using Plotly
fig = px.line(df_all_stocks, x="Date", y=df_all_stocks.columns,
              hover_data={"Date": "|%Y,%m,%d"},
              title=' Stock Portfolio')
fig.update_xaxes(dtick="M1", tickformat="%b\n%Y")
fig.show()

# Commit and Close Database
conn.commit()
conn.close()


