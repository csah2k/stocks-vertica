
# https://plotly.com/
# pip3 install dash pandas pandas_datareader

import plotly.express as px # or plotly.express as px

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

server_host = '0.0.0.0'
server_port = 8050
server_workers = 5

conn_info = {'host': '192.168.1.180',
            'port': 5433,
            'user': 'dbadmin',
            'password': 'stocks',
            'database': 'stocks',
            'use_prepared_statements': False}

db_schema = "stocks"
input_table = "daily_prices"
input_columns = ["open", "close", "high", "low", "volume"]
output_columns = ["open", "close", "high", "low", "volume"]

new_auto_connection(conn_info, name = "VerticaDSN")
change_auto_connection("VerticaDSN")

vertica_connection = vertica_python.connect(**conn_info)
#vert_cur = vertica_connection.cursor()


app = dash.Dash('Stocks', external_stylesheets=['https://codepen.io/chriddyp/pen/bWLwgP.css'])


def listSymbols(table_name="stock_symbols"):
    with vertica_connection.cursor() as vert_cur:
        symbols = []    
        for row in vert_cur.execute(f"SELECT DISTINCT company, symbol, industry, headquarters FROM {getRelation(table_name)} WHERE symbol in (select symbol from {getRelation(input_table)}) ;").fetchall():
            symbols.append({'label': f"{row[0]} ({row[1]})", 'value': row[1]})
        return symbols

def getDbLastTimestamp(symbol, table_name=input_table, column_name="ts"):  
    with vertica_connection.cursor() as vert_cur:
        return vert_cur.execute(f"SELECT COALESCE(MAX(\"{column_name}\"), '1990-01-01') as \"{column_name}\" FROM {getRelation(table_name)} WHERE symbol = '{symbol}' and close is not null;").fetchone()[0]
    

def getRelation(table=input_table, schema=db_schema, table_only=False):
    table_search = re.search(r"(\"?(\w+)\"?\.)?\"?(\w+)\"?", table, re.IGNORECASE)
    if table_search:
        table = table_search.group(3)
    if table_only: return table
    else: return f"\"{schema}\".\"{table}\"" 

def symbolToTable(symbol):
    return re.sub(r'[^A-Z09]', '', symbol).lower().strip()

@app.callback(Output('price-graph', 'figure'), [
    Input('symbol-dropdown', 'value'), 
    Input('period-dropdown', 'value'), 
    Input('smoothing-dropdown', 'value'),
    Input('analitcs-dropdown', 'value')
    ])
def update_graph(selected_symbol, selected_period, n, analitcs):
    vdf = vDataFrame(getRelation(f"daily_prices_{symbolToTable(selected_symbol)}_simulation"))
    print(analitcs)

    shapes = []
    last_real_data = getDbLastTimestamp(selected_symbol)
    print(f"last_real_data: {last_real_data}")
    if "LR" in analitcs:
        shapes.append({
            'x0': last_real_data, 
            'x1': last_real_data, 
            'y0': 0, 
            'y1': 1, 
            'xref': 'x', 
            'yref': 'paper',
            'line_width': 1
        })        
    else:
        vdf.filter(conditions = [f"ts <= '{last_real_data}'"])

    

    #vdf.eval(name = "real_volume", expr=f"CASE WHEN ts <= '{last_real_data}' THEN volume/10000000 ELSE Null END")
    #vdf.eval(name = "predicted_volume", expr=f"CASE WHEN ts >= '{last_real_data}' THEN volume/10000000 ELSE Null END")
    
    
    if "ROCP" in analitcs or "ROCV" in analitcs or "RSI" in analitcs:
        vdf.eval(name = "_close_D1_0", expr = "LAG(close, 1, 0) OVER(PARTITION BY symbol ORDER BY ts)")
        # https://www.fmlabs.com/reference/default.htm?url=RSI.htm
        if "RSI" in analitcs:
            vdf.eval(name = "_up", expr = "CASE WHEN close > _close_D1_0 THEN close - _close_D1_0 ELSE 0 END")
            vdf.eval(name = "_dn", expr = "CASE WHEN close > _close_D1_0 THEN 0 ELSE _close_D1_0 - close END")
            vdf.eval(name = "_upavg", expr = f"AVG(_up) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '{n} days' PRECEDING AND CURRENT ROW)")
            vdf.eval(name = "_dnavg", expr = f"AVG(_dn) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '{n} days' PRECEDING AND CURRENT ROW)")
            vdf.eval(name = "RSI", expr = "ROUND(100 * ( _upavg / ( _upavg + _dnavg )), 2)")

        if "ROCP" in analitcs or "ROCV" in analitcs:
            vdf.eval(name = "_close_D1_1", expr = "LAG(close, 1, 1) OVER(PARTITION BY symbol ORDER BY ts)")
            vdf.eval(name = "_roc", expr = "(close - _close_D1_0 ) / _close_D1_1 * 100")
            #  https://www.investopedia.com/terms/p/pricerateofchange.asp
            if "ROCP" in analitcs:
                vdf.eval(name = "roc_long_ema", expr = "ROUND(EXPONENTIAL_MOVING_AVERAGE(_roc, 0.075) OVER (PARTITION BY symbol ORDER BY ts), 2)")
            if "ROCV" in analitcs:
                vdf.eval(name = "roc_volume", expr=f"ABS(ROUND(volume/1000000, 1))")
                vdf.normalize(["roc_volume"], "minmax")

                vdf.eval(name = "roc_volume_buy", expr=f"CASE WHEN _roc > 0 THEN ABS(roc_volume) * 100 ELSE Null END")
                vdf.eval(name = "roc_volume_sell", expr=f"CASE WHEN _roc <= 0 THEN ABS(roc_volume) * -100 ELSE Null END")
    #vdf.eval(name = "roc_color", expr=f"CASE WHEN _roc > 0 THEN 'green' ELSE 'red' END")

    # https://www.investopedia.com/terms/b/bollingerbands.asp
    #n = 20 # Number of days in smoothing period (typically 20)
    m = 2.0  # Number of standard deviations (typically 2)
    if "BOL" in analitcs:
        vdf.eval(name = "_TP", expr = "apply_avg(ARRAY[ high, low, close ])") # typical price
        vdf.eval(name = "_TP_DEV", expr = f"STDDEV(_TP) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '{n} days' PRECEDING AND CURRENT ROW)") # Standard Deviation over last n periods of TP
        vdf.eval(name = "BOLM", expr = f"AVG(_TP) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '{n} days' PRECEDING AND CURRENT ROW)") # typical price Moving average - Middle Bollinger Band
        vdf.eval(name = "BOLU", expr = f"ROUND(BOLM + ({m} * _TP_DEV), 2)") # Upper Bollinger Band
        vdf.eval(name = "BOLD", expr = f"ROUND(BOLM - ({m} * _TP_DEV), 2)") # Lower Bollinger Band




    # https://www.investopedia.com/terms/v/vwap.asp
    if "VWAP" in analitcs:
        vdf.eval(name = "_PV", expr = "volume * apply_avg(ARRAY[high, low, close])")
        vdf.eval(name = "VWAP", expr = "ROUND(_PV / volume, 2)")

    # https://www.fmlabs.com/reference/default.htm?url=StochasticOscillator.htm
    '''
    vdf.eval(name = "highest_high_1M", expr = "MAX(high) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '1 month' PRECEDING AND CURRENT ROW)")
    vdf.eval(name = "lowest_low_1M", expr = "MIN(low) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '1 month' PRECEDING AND CURRENT ROW)")
    vdf.eval(name = "_k_1M", expr = "100 * ((close - lowest_low_1M ) / (highest_high_1M - lowest_low_1M))")
    vdf.eval(name = "STOCH_1M", expr = "EXPONENTIAL_MOVING_AVERAGE(_k_1M, 0.15) OVER (PARTITION BY symbol ORDER BY ts)")

    vdf.eval(name = "highest_high_3M", expr = "MAX(high) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '3 month' PRECEDING AND CURRENT ROW)")
    vdf.eval(name = "lowest_low_3M", expr = "MIN(low) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '3 month' PRECEDING AND CURRENT ROW)")
    vdf.eval(name = "_k_3M", expr = "100 * ((close - lowest_low_3M ) / (highest_high_3M - lowest_low_3M ))")    
    vdf.eval(name = "STOCH_3M", expr = "EXPONENTIAL_MOVING_AVERAGE (_k_3M , 0.075) OVER (PARTITION BY symbol ORDER BY ts)")
    '''


    # TODO adicionar MACD
    # https://www.investopedia.com/terms/m/macd.asp
    # TODO adicionar grafico de volume barras separado em baixo
    # https://www.avatradeportuguese.com/education/trading-for-beginners/bandas-de-bollinger


    vdf.filter(conditions = [f"ts >= ADD_MONTHS(CURRENT_TIMESTAMP, -{selected_period})"])
    
    select_columns = [str(c).replace('"','') for c in vdf.get_columns()]
    vdf = vdf.select([c for c in select_columns if not c.startswith("_")], False)

    vdf.sort({"ts": "desc"})

    df = vdf.to_pandas()
    print(df)

    # https://plotly.com/python/statistical-charts/
    data = [{
            'x':df['ts'],
            'open':df['open'],
            'high':df['high'],
            'low':df['low'],
            'close':df['close'],
            #'text':df['ts'],
            #'hoverinfo': 'text',
            'type': 'candlestick',
            'name': 'Valor do Ativo (R$)'
        }]

    '''
    {
        'x': df.ts,
        'y': df.pred_close,
        'name': 'Simulação (R$)',
        'type': 'line',
        'yaxis': 'y1',
        'line': {'color': 'darkorange', 'width': 3}
    }
    '''

    if "ROCP" in analitcs:
        data.append({
                'x': df.ts,
                'y': df.roc_long_ema,
                'name': 'ROC (%)',
                'type': 'line',
                'yaxis': 'y2',
                'line': { 'color': 'darkcyan', 'width': 2}
            })

    if "ROCV" in analitcs:
        data.append({
                'x': df.ts,
                'y': df.roc_volume_buy,
                'line': { 'color': '#158467', 'width': 2},
                'name': 'Vol. Buy (%)',
                'type': 'bar',
                'yaxis': 'y2'
            })
        data.append({
                'x': df.ts,
                'y': df.roc_volume_sell,
                'line': { 'color': '#810000', 'width': 2},
                'name': 'Vol. Sell (%)',
                'type': 'bar',
                'yaxis': 'y2'
            })

    if "BOL" in analitcs:
        data.append({
                'x': df.ts,
                'y': df.BOLU,
                'name': 'Upper B.B. (R$)',
                'type': 'line',
                'yaxis': 'y1',
                'line': { 'color': 'green', 'width': 1, 'dash': 'dot'}
            })
        data.append({
                'x': df.ts,
                'y': df.BOLM,
                'name': 'Center B.B. (R$)',
                'type': 'line',
                'yaxis': 'y1',
                'line': { 'color': 'grey', 'width': 1, 'dash': 'dot'}
            })
        data.append({
                'x': df.ts,
                'y': df.BOLD,
                'name': 'Lower B.B. (R$)',
                'type': 'line',
                'yaxis': 'y1',
                'line': { 'color': 'crimsom', 'width': 1, 'dash': 'dot'}
            })


    if "RSI" in analitcs:
        data.append({
                'x': df.ts,
                'y': df.RSI,
                'name': 'RSI (%)',
                'type': 'line',
                'yaxis': 'y2',
                'line': { 'color': 'blue', 'width': 1}
            })

    if "VWAP" in analitcs:
        data.append({
                'x': df.ts,
                'y': df.VWAP,
                'name': 'VWAP (%)',
                'type': 'line',
                'yaxis': 'y2',
                'line': { 'color': '#a35d6a', 'width': 1}
            })

    # ================= LAYOUT ================= #
    layout = {
            'margin': {'l': 70, 'r': 70, 't': 70, 'b': 70},
            'title': f"{selected_symbol} ({selected_period} Meses)",
            'height': 800,
            'shapes': shapes,
            'yaxis': {
                'title' : "R$",
                'titlefont': {
                    'color': "#1f77b4"
                },
                'tickfont': {
                    'color': "#1f77b4"
                },
                'tickprefix': "R$ "
            },
            'yaxis2': {
                'title' : "%",
                'titlefont': {
                    'color': "#1f77b4"
                },
                'tickfont': {
                    'color': "#1f77b4"
                },
                'tickprefix': "% ",
                #'anchor': 'free',
                'overlaying': 'y',
                'side': 'right',
                #'position': 0.15,
            }
        }
    
    return {
        'data': data, 
        'layout': layout
    }   




# ================


all_symbols = listSymbols()
app.layout = html.Div(children=[

    html.Div(children=[
        
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
                {'label': '3 Meses', 'value': '3'},
                {'label': '6 Meses', 'value': '6'},
                {'label': '1 Ano', 'value': '12'},
                {'label': '2 Anos', 'value': '24'}
            ],  
            value='6',
            clearable=False
        ),
        
        html.Label('Number of days in smoothing period (typically 20)'),
        dcc.Dropdown(
            id='smoothing-dropdown',
            options=[
                {'label': '10 Dias', 'value': '10'},
                {'label': '15 Dias', 'value': '15'},
                {'label': '20 Dias', 'value': '20'},
                {'label': '25 Dias', 'value': '25'},
                {'label': '30 Dias', 'value': '30'}
            ],  
            value='20',
            clearable=False
        ),

        html.Label('Indicadores'),
        dcc.Dropdown(
            id='analitcs-dropdown',
            options=[
                {'label': 'Linear Regression Simulation (LR)', 'value': 'LR'},
                {'label': 'Bollinger Bands (BOL)', 'value': 'BOL'},
                {'label': 'Price Rate of Change (ROC)', 'value': 'ROCP'},
                {'label': 'Volume ROC (ROCV)', 'value': 'ROCV'},
                {'label': 'Relative Strength Index (RSI)', 'value': 'RSI'},
                {'label': 'Volume Weighted Average (VWAP)', 'value': 'VWAP'}
            ],  
            multi=True,
            value=['LR', 'BOL', 'RSI'],
            clearable=False
        ),

    ]),


    html.Div(children=[
            # https://dash.plotly.com/dash-core-components/graph
            dcc.Graph(
                id='price-graph',
                animate=False,
                config={
                    'scrollZoom': False, 
                    'showAxisDragHandles': True
                    }
                ),
        ]),


])





if __name__ == '__main__':
    app.run_server(debug=False, use_reloader=True, host=server_host, port=server_port, threaded=True)
