
# https://plotly.com/
# pip3 install dash pandas pandas_datareader


import dash
from dash.dependencies import Input, Output
import dash_core_components as dcc 
import dash_html_components as html

from pandas_datareader import data as web 
from datetime import datetime as dt

import verticapy
from verticapy import *
from verticapy.toolbox import *
from verticapy.connections.connect import *

conn_info = {'host': '127.0.0.1',
            'port': 5433,
            'user': 'dbadmin',
            'password': 'stocks',
            'database': 'stocks'}

new_auto_connection(conn_info, method = "vertica_python", name = "VerticaDSN")
change_auto_connection("VerticaDSN")


app = dash.Dash('Stocks', external_stylesheets=['https://codepen.io/chriddyp/pen/bWLwgP.css'])

app.layout = html.Div([
    dcc.Dropdown(
        id='my-dropdown',
        options=[
            {'label': 'Petrobrás', 'value': 'PETR4.SA'},
            {'label': 'CVC', 'value': 'CVCB3.SA'},
            {'label': 'Itaú', 'value': 'ITUB4.SA'}
        ],  
        value='PETR4'
    ),  
    dcc.Graph(id='my-graph')
], style={'width': '500'})

def symbolToTable(symbol):
    return re.sub(r'[^A-Z09]', '', symbol).lower().strip()

@app.callback(Output('my-graph', 'figure'), [Input('my-dropdown', 'value')])
def update_graph(selected_dropdown_value):
    vdf = vDataFrame(f"public.daily_prices_{symbolToTable(selected_dropdown_value)}_SIMULATION")
    vdf.sort({"ts": "desc"})
    df = vdf.to_pandas()
    return {
        'data': [{
            'x': df.ts,
            'y': df.close
        }], 
        'layout': {'margin': {'l': 40, 'r': 0, 't': 20, 'b': 30}}
    }   

if __name__ == '__main__':
    app.run_server()