# -*- coding: utf-8 -*-
"""
Created on Fri Jan 29 13:27:05 2021

@author: Irasema
"""
'''
PREREQUISITOS. 
1. Ejecutar el script stocks.sql en SQL Server (local o de Azure)
2. Si ya se ha ejecutado antes este programa o se quiere reiniciar la BD, hacer 'delete from stocks_history'
3. pip install pyodbc (Driver para SQL Server)
4. pip install yfinance (Yahoo Finance)
'''

import pyodbc
from datetime import datetime
import datetime
from datetime import timedelta
import pandas as pd
import pandas_datareader.data as web

'''
PARTE 1. Conectar a BD SQL SERVER y extraer de la tabla stocks los tickets de las acciones
'''

# Credenciales para conexión a Azure SQL Server
conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=tfmcice.database.windows.net;'
                      'Database=StocksDB;'
                      'UID=tfmcice;'
                      'PWD=xxxxxxx;'
                      'Trusted_Connections=no;')

cursor = conn.cursor()

cursor.execute('''select s.stock, max(sh.stock_date) 
               from stocks s left join stocks_history sh on s.stock = sh.stock
               group by s.stock''') 
row = cursor.fetchone() 
stocks = []
while row: 
    # Columna 0: Ticket, Columna 1: Fecha del ultimo precio registrado
    stock_detail = { 'stock' : row[0], 'max_date': row[1]}
    stocks.append(stock_detail)
    row = cursor.fetchone()

print("Total de acciones a recuperar:", len(stocks))

'''
PARTE 2. Conectar a API de yfinance
'''

ONE_DAY = 1
YAHOO = "yahoo"
DEFAULT_START_DATE = datetime.datetime(2016, 1, 1)

for i in range(len(stocks)):
    # Definir una fecha inicio y fecha fin para cada stock
    end_date = datetime.datetime.now()
    # El inicio será el valor máximo en BD + 1 día, o en su defecto, 01-ene-2021
    start_date = stocks[i].get("max_date")
    if start_date == None: 
        start_date = DEFAULT_START_DATE
    else:
        start_date += timedelta(days = ONE_DAY)
    
    # Si esta condición ocurre es que tenemos el precio de la acción al día y no debemos actualizar
    if start_date > end_date:
        print("'{}' ya está actualizado".format(stocks[i].get("stock")))
    else:
        print("Obteniendo precios para '{}' en el periodo {} - {}".format(stocks[i].get("stock"), start_date, end_date))
        df_history = web.DataReader(stocks[i].get("stock"), YAHOO, start_date, end_date)
        # La fecha del precio es recibida como indice del dataframe, la pasamos a una columna
        df_history['Date'] = df_history.index
        # Quitamos espacios en blanco de los nombres de columnas. Ej. Adj Close -> AdjClose
        df_history.columns = df_history.columns.str.replace(' ', '')
        
        # Insertamos dataframe a SQL Server:
        for index, row in df_history.iterrows():
            cursor.execute("insert into stocks_history (stock, stock_date, high_value, low_value, open_value, close_value, volume, adj_close) values(?,?,?,?,?,?,?,?)",
                           stocks[i].get("stock"), row.Date, row.High, row.Low, row.Open, row.Close, row.Volume, row.AdjClose)
        conn.commit()
    
cursor.close()
print("Stocks actualizados")

