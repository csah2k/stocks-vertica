
# https://plotly.com/
# pip3 install dash pandas pandas_datareader


import dash
from dash.dependencies import Input, Output
import dash_core_components as dcc 
import dash_html_components as html

from pandas_datareader import data as web 
from datetime import datetime as dt

import vertica_python
import verticapy
from verticapy import *
from verticapy.toolbox import *
from verticapy.connections.connect import *

conn_info = {'host': '127.0.0.1',
            'port': 5433,
            'user': 'dbadmin',
            'password': 'stocks',
            'database': 'stocks',
            'use_prepared_statements': True}

db_schema = "stocks"
input_table = "daily_prices"
input_columns = ["open", "close", "high", "low", "volume"]
output_columns = ["open", "close", "high", "low", "volume"]

new_auto_connection(conn_info, method = "vertica_python", name = "VerticaDSN")
change_auto_connection("VerticaDSN")

vertica_connection = vertica_python.connect(**conn_info)
vert_cur = vertica_connection.cursor()

app = dash.Dash('Stocks', external_stylesheets=['https://codepen.io/chriddyp/pen/bWLwgP.css'])


def listSymbols(table_name="stock_symbols"):
    symbols = []
    if table_name.find(f"\"{db_schema}\"") < 0:
        table_name = f"\"{db_schema}\".\"{table_name}\""        
    for row in vert_cur.execute(f"SELECT DISTINCT company, symbol, industry, headquarters FROM {table_name};").fetchall():
        symbols.append({'label': row[0], 'value': row[1]})
    return symbols

def getDbLastTimestamp(symbol, table_name="daily_prices", column_name="ts"):
    if table_name.find(f"\"{db_schema}\"") < 0:
        table_name = f"\"{db_schema}\".\"{table_name}\""        
    return vert_cur.execute(f"SELECT COALESCE(MAX(\"{column_name}\"), '1990-01-01') as \"{column_name}\" FROM {table_name} WHERE symbol = '{symbol}' and close is not null;").fetchone()[0]


def symbolToTable(symbol):
    return re.sub(r'[^A-Z09]', '', symbol).lower().strip()

@app.callback(Output('my-graph', 'figure'), [Input('symbol-dropdown', 'value'), Input('period-dropdown', 'value')])
def update_graph(selected_symbol, selected_period):
    vdf = vDataFrame(f"\"{db_schema}\".\"daily_prices_{symbolToTable(selected_symbol)}_simulation\"")
    vdf.filter(conditions = [f"ts >= ADD_MONTHS(CURRENT_TIMESTAMP, -{selected_period})"])
    vdf.sort({"ts": "desc"})
    
    last_real_data = getDbLastTimestamp(selected_symbol)
    vdf.eval(name = "real_close", expr=f"CASE WHEN ts <= '{last_real_data}' THEN close ELSE Null END")
    vdf.eval(name = "predicted_close", expr=f"CASE WHEN ts >= '{last_real_data}' THEN close ELSE Null END")

    df = vdf.to_pandas()
    print(df)

    return {
        'data': [
            {
                'x': df.ts,
                'y': df.real_close,
                'label': 'Histórico'
            },
            {
                'x': df.ts,
                'y': df.predicted_close,
                'label': 'Previsão'
            }
        ], 
        'layout': {'margin': {'l': 40, 'r': 0, 't': 20, 'b': 30}}
    }   



# ================

all_symbols = listSymbols()
app.layout = html.Div([
    dcc.Dropdown(
        id='symbol-dropdown',
        options=all_symbols,  
        value=all_symbols[0]['value'],
        clearable=False
    ),
    dcc.Dropdown(
        id='period-dropdown',
        options=[
            {'label': '1 Mês', 'value': '1'},
            {'label': '3 Mêses', 'value': '3'},
            {'label': '6 Mêses', 'value': '6'},
            {'label': '1 Ano', 'value': '12'},
            {'label': '2 Anos', 'value': '24'}
        ],  
        value='6',
        clearable=False
    ),  
    dcc.Graph(id='my-graph')
])



if __name__ == '__main__':
    app.run_server()