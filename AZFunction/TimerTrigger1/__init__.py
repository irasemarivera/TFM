from datetime import datetime
from datetime import timedelta
from datetime import time
import datetime
import logging
import pyodbc
import pandas as pd
import pandas_datareader.data as web
import azure.functions as func
import os


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()
    logging.info('Hora de ejecución: %s', utc_timestamp)

    '''
    PREREQUISITOS. 
    1. Ejecutar el script stocks.sql en SQL Server
    2. Si ya se ha ejecutado antes este programa o se quiere reiniciar la BD, hacer 'delete from stocks_history'
    3. pip install pyodbc (Driver para SQL Server)
    4. pip install yfinance (Yahoo Finance)
    '''

    '''
    PARTE 1. Conectar a BD SQL SERVER y extraer de la tabla stocks los tickets de las acciones
    '''

    # Conexión a Azure SQL Server
    conn = pyodbc.connect(os.environ["SQLConnectionString"])

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

    logging.info("Total de acciones a recuperar: {}".format(len(stocks)))

    '''
    PARTE 2. Conectar a API de yfinance y guardar en BD
    '''

    ONE_DAY = 1
    YAHOO = "yahoo"
    DEFAULT_START_DATE = datetime.datetime(2016, 1, 1).replace(tzinfo=datetime.timezone.utc)

    for i in range(len(stocks)):
        # Definir una fecha inicio y fecha fin para cada stock
        # La fecha fin en principio debe ser la fecha de hoy a las 00:00hrs
        today = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
        end_date = datetime.datetime(today.year, today.month, today.day).replace(tzinfo=datetime.timezone.utc)
        # El inicio será el valor máximo en BD + 1 día, o en su defecto, 01-ene-2016
        max_date = stocks[i].get("max_date")
        if max_date == None: 
            start_date = DEFAULT_START_DATE
        else:
            start_date = max_date.replace(tzinfo=datetime.timezone.utc) + timedelta(days = ONE_DAY)
        
        # Si esta condición ocurre significa que tenemos el precio de la acción al día y no debemos actualizar
        if start_date >= end_date:
            logging.info("'{}' ya está actualizado".format(stocks[i].get("stock")))
        else:
            logging.info("Obteniendo precios para '{}' en el periodo {} - {}".format(stocks[i].get("stock"), start_date, end_date))
            df_history = web.DataReader(stocks[i].get("stock"), YAHOO, start_date, end_date)
            # La fecha del precio es recibida como indice del dataframe, la pasamos a una columna
            df_history['Date'] = df_history.index
            # Quitamos espacios en blanco de los nombres de columnas. Ej. Adj Close -> AdjClose
            df_history.columns = df_history.columns.str.replace(' ', '')
            
            # Insertamos dataframe a SQL Server:
            for index, row in df_history.iterrows():
                #En fin de semana llega el registro del viernes, aunque se consulte de sabado a domingo
                #por lo que si el registro que llega es el mismo que el maximo en BD no se inserta
                if max_date != row.Date:
                    cursor.execute("insert into stocks_history (stock, stock_date, high_value, low_value, open_value, close_value, volume, adj_close) values(?,?,?,?,?,?,?,?)",
                            stocks[i].get("stock"), row.Date, row.High, row.Low, row.Open, row.Close, row.Volume, row.AdjClose)
                else:
                    logging.info("Fila no insertada porque coincide con el ultimo registro de la base de datos: {}".format(row.Date))
            conn.commit()
        
    cursor.close()
    logging.info("Stocks actualizados")
