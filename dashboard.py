
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
    for row in vert_cur.execute(f"SELECT DISTINCT company, symbol, industry, headquarters FROM {getRelation(table_name)} WHERE symbol in (select symbol from {getRelation(input_table)}) ;").fetchall():
        symbols.append({'label': f"{row[0]} ({row[1]})", 'value': row[1]})
    return symbols

def getDbLastTimestamp(symbol, table_name=input_table, column_name="ts"):      
    return vert_cur.execute(f"SELECT COALESCE(MAX(\"{column_name}\"), '1990-01-01') as \"{column_name}\" FROM {getRelation(table_name)} WHERE symbol = '{symbol}' and close is not null;").fetchone()[0]

def getRelation(table=input_table, schema=db_schema, table_only=False):
    table_search = re.search(r"(\"?(\w+)\"?\.)?\"?(\w+)\"?", table, re.IGNORECASE)
    if table_search:
        table = table_search.group(3)
    if table_only: return table
    else: return f"\"{schema}\".\"{table}\"" 

def symbolToTable(symbol):
    return re.sub(r'[^A-Z09]', '', symbol).lower().strip()

@app.callback(Output('price-graph', 'figure'), [Input('symbol-dropdown', 'value'), Input('period-dropdown', 'value')])
def update_graph(selected_symbol, selected_period):
    vdf = vDataFrame(getRelation(f"daily_prices_{symbolToTable(selected_symbol)}_simulation"))

    last_real_data = getDbLastTimestamp(selected_symbol)
    vdf.eval(name = "real_close", expr=f"CASE WHEN ts <= '{last_real_data}' THEN ROUND(close, 2) ELSE Null END")
    vdf.eval(name = "predicted_close", expr=f"CASE WHEN ts >= '{last_real_data}' THEN ROUND(close, 2) ELSE Null END")
    #vdf.eval(name = "real_volume", expr=f"CASE WHEN ts <= '{last_real_data}' THEN volume/10000000 ELSE Null END")
    #vdf.eval(name = "predicted_volume", expr=f"CASE WHEN ts >= '{last_real_data}' THEN volume/10000000 ELSE Null END")
    
    #  https://www.investopedia.com/terms/p/pricerateofchange.asp
    vdf.eval(name = "close_D1_0", expr = "LAG(close, 1, 0) OVER(PARTITION BY symbol ORDER BY ts)")
    vdf.eval(name = "close_D1_1", expr = "LAG(close, 1, 1) OVER(PARTITION BY symbol ORDER BY ts)")
    vdf.eval(name = "roc", expr = "(close - close_D1_0 ) / close_D1_1 * 100")
    vdf.eval(name = "roc_long_ema", expr = "ROUND(EXPONENTIAL_MOVING_AVERAGE(roc, 0.075) OVER (PARTITION BY symbol ORDER BY ts), 2)")

    # https://www.investopedia.com/terms/b/bollingerbands.asp
    n = 20 # Number of days in smoothing period (typically 20)
    m = 2  # Number of standard deviations (typically 2)
    vdf.eval(name = "TP", expr = "apply_avg(ARRAY[ high, low, close ])") # typical price
    vdf.eval(name = "TP_DEV", expr = f"STDDEV(TP) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '{n} days' PRECEDING AND CURRENT ROW)") # Standard Deviation over last n periods of TP
    vdf.eval(name = "TP_MA", expr = f"AVG(TP) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '{n} days' PRECEDING AND CURRENT ROW)") # typical price Moving average
    vdf.eval(name = "BOLU", expr = f"ROUND(TP_MA + ({m} * TP_DEV), 2)") # Upper Bollinger Band
    vdf.eval(name = "BOLD", expr = f"ROUND(TP_MA - ({m} * TP_DEV), 2)") # Lower Bollinger Band

    # https://www.fmlabs.com/reference/default.htm?url=RSI.htm
    vdf.eval(name = "_up", expr = "CASE WHEN close > close_D1_0 THEN close - close_D1_0 ELSE 0 END")
    vdf.eval(name = "_dn", expr = "CASE WHEN close > close_D1_0 THEN 0 ELSE close_D1_0 - close END")
    vdf.eval(name = "_upavg_1M", expr = "AVG(_up) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '1 month' PRECEDING AND CURRENT ROW)")
    vdf.eval(name = "_dnavg_1M", expr = "AVG(_dn) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '1 month' PRECEDING AND CURRENT ROW)")
    vdf.eval(name = "RMI_1M", expr = "ROUND(100 * ( _upavg_1M / ( _upavg_1M - _dnavg_1M )), 2)")
    #vdf.eval(name = "_upavg_3M", expr = "AVG(_up) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '3 month' PRECEDING AND CURRENT ROW)")
    #vdf.eval(name = "_dnavg_3M", expr = "AVG(_dn) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '3 month' PRECEDING AND CURRENT ROW)")
    #vdf.eval(name = "RMI_3M", expr = "100 * ( _upavg_3M / ( _upavg_3M - _dnavg_3M ))")


    vdf.filter(conditions = [f"ts >= ADD_MONTHS(CURRENT_TIMESTAMP, -{selected_period})"])
    vdf.sort({"ts": "desc"})

    df = vdf.to_pandas()

    return {
        'data': [
            {
                'x': df.ts,
                'y': df.real_close,
                'name': 'Histórico',
                'type': 'line',
                'line': { 'color': 'blue', 'width': 3}
            },
            {
                'x': df.ts,
                'y': df.predicted_close,
                'name': 'Simulação',
                'type': 'line',
                'line': { 'color': 'darkorange', 'width': 3}
            },
            {
                'x': df.ts,
                'y': df.BOLU,
                'name': 'Upper Band',
                'type': 'line',
                'line': { 'color': 'green', 'width': 1}
            },
            {
                'x': df.ts,
                'y': df.BOLD,
                'name': 'Lower Band',
                'type': 'line',
                'line': { 'color': 'crimsom', 'width': 1}
            },
            {
                'x': df.ts,
                'y': df.roc_long_ema,
                'name': 'Rate of Change',
                'type': 'line',
                'line': { 'color': 'darkcyan', 'width': 2}
            }
        ], 
        'layout': {
            'margin': {'l': 40, 'r': 40, 't': 50, 'b': 30},
            'title': f"Stocks prices Dashboard"
        }
    }   




# ================


all_symbols = listSymbols()
app.layout = html.Div(style={}, children=[
    
    html.Div(style={'columnCount': 1}, children=[
        # https://dash.plotly.com/dash-core-components/graph
        dcc.Graph(
            id='price-graph',
            animate=True,
            config={
                'scrollZoom': True, 
                'showAxisDragHandles': True
                }
            ),
    ]),

    html.Div(style={'columnCount': 1}, children=[
        html.Label('Ativo'),
        dcc.Dropdown(
            id='symbol-dropdown',
            options=all_symbols,  
            value=all_symbols[0]['value'],
            clearable=False
        ),

        html.Label('Período'),
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
        )
    ]),


])





if __name__ == '__main__':
    app.run_server()