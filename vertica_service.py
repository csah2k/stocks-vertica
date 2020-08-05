#%%
# pip3 install vertica-python verticapy holidays
import datetime
import holidays
import vertica_python
import verticapy
from verticapy import *
from verticapy.toolbox import *
from verticapy.connections.connect import *
from verticapy.learn.linear_model import LinearRegression
from finnhub import Client

ONE_DAY = datetime.timedelta(days=1)
HOLIDAYS_BR = holidays.BR()

finnhub_client = Client(api_key="bqu00jvrh5rb3gqqf9q0")

conn_info = {'host': '127.0.0.1',
            'port': 5433,
            'user': 'dbadmin',
            'password': 'stocks',
            'database': 'stocks'}

input_table = "public.daily_prices"
input_columns = ["open", "close", "high", "low", "volume"]
output_columns = ["open", "close", "high", "low", "volume"]

new_auto_connection(conn_info, method = "vertica_python", name = "VerticaDSN")
change_auto_connection("VerticaDSN")

vertica_connection = vertica_python.connect(**conn_info)
vert_cur = vertica_connection.cursor()

def runProcess():
    symbols = [ 'ABEV3',
                'B3SA3',
                'BBAS3',
                'BBDC4',
                'BBSE3',
                'BOVA11',
                'BRFS3',
                'BRKM5',
                'BRML3',
                'CCRO3',
                'CIEL3',
                'CMIG4',
                'COGN3',
                'CSAN3',
                'CSNA3',
                'CYRE3',
                'ECOR3',
                'EGIE3',
                'ELET3',
                'EMBR3',
                'EQTL3',
                'GGBR4',
                'GOAU4',
                'HYPE3',
                'IRBR3',
                'ITSA4',
                'ITUB4',
                'JBSS3',
                'LAME4',
                'LREN3',
                'MGLU3',
                'MRFG3',
                'NTCO3',
                'CVCB3',
                'PETR4',
                'QUAL3',
                'RADL3',
                'RAIL3',
                'RENT3',
                'SBSP3',
                'SUZB3',
                'TAEE11',
                'USIM5',
                'VALE3',
                'VIVT4',
                'VVAR3',
                'WEGE3',
                'YDUQ3']

    for target_symbol in symbols:
        models = trainSymbolModels(target_symbol)
        simulateSymbolData(target_symbol, models, 20)


def next_business_day(ref_date=datetime.date.today()):
    next_day = ref_date + ONE_DAY
    while next_day.weekday() in holidays.WEEKEND or next_day in HOLIDAYS_BR:
        next_day += ONE_DAY
    return next_day

def getDbLastTimestamp(symbol, table_name="daily_prices", column_name="ts"):
    return vert_cur.execute(f"SELECT MAX(\"{column_name}\") as \"{column_name}\" FROM {table_name} WHERE symbol = '{symbol}.SA' and close is not null;").fetchone()[0]

def mergeSimulationData(simulation_table, simulation_result_table):
    return vert_cur.execute(f"MERGE INTO {simulation_table} sim USING {simulation_result_table} res ON sim.ts = res.ts WHEN MATCHED THEN UPDATE SET open = res.open, close = res.close, high = res.high, low = res.low, volume = res.volume WHEN NOT MATCHED THEN INSERT (ts, symbol, open, close, high, low, volume) VALUES (res.ts, res.symbol, res.open, res.close, res.high, res.low, res.volume);").fetchone()

def dropTable(table_name, cascade=True):
    sql = f"DROP TABLE IF EXISTS {table_name}"
    if cascade: sql = f"{sql} CASCADE;"
    return vert_cur.execute(sql).fetchone()

def dropView(view_name):
    sql = f"DROP VIEW IF EXISTS {view_name};"
    return vert_cur.execute(sql).fetchone()


def generateVariables(vdf):
    #  https://www.investopedia.com/terms/p/pricerateofchange.asp
    #vdf.eval(name = "roc", expr = f"((close - (LAG(close, 1, 0) OVER (PARTITION BY symbol ORDER BY ts)) )/(LAG(close, 1, 1) OVER (PARTITION BY symbol ORDER BY ts))) * 100")
    #if "roc3" not in input_columns:
    #vdf.eval(name = "roc3", expr = f"close - (LAG(close, 3, 0) OVER (PARTITION BY symbol ORDER BY ts))")
    #input_columns.append("roc3")
    for col in input_columns:
        #print(f"Generatig derivatives from '{col}'...")
        # https://www.investopedia.com/terms/m/macd.asp
        # https://www.investopedia.com/terms/m/movingaverage.asp
        # https://www.mssqltips.com/sqlservertip/5441/using-tsql-to-detect-golden-crosses-and-death-crosses/
        vdf.eval(name = f"{col}_ema_01", expr = f"EXPONENTIAL_MOVING_AVERAGE({col}, 0.1) OVER (PARTITION BY symbol ORDER BY ts)")
        vdf.eval(name = f"LAG_{col}_ema_01", expr = f"LAG({col}_ema_01, 1, Null) OVER(PARTITION BY symbol ORDER BY ts)")
        
        vdf.eval(name = f"{col}_ema_03", expr = f"EXPONENTIAL_MOVING_AVERAGE({col}, 0.3) OVER (PARTITION BY symbol ORDER BY ts)")
        vdf.eval(name = f"LAG_{col}_ema_03", expr = f"LAG({col}_ema_03, 1, Null) OVER(PARTITION BY symbol ORDER BY ts)")

        vdf.eval(name = f"{col}_sma_1W", expr = f"AVG({col}) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '1 week' PRECEDING AND CURRENT ROW)")
        vdf.eval(name = f"LAG_{col}_sma_1W", expr = f"LAG({col}_sma_1W, 1, Null) OVER(PARTITION BY symbol ORDER BY ts)")

        vdf.eval(name = f"{col}_sma_1M", expr = f"AVG({col}) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '1 month' PRECEDING AND CURRENT ROW)")
        vdf.eval(name = f"LAG_{col}_sma_1M", expr = f"LAG({col}_sma_1M, 1, Null) OVER(PARTITION BY symbol ORDER BY ts)")

        vdf.eval(name = f"{col}_sma_3M", expr = f"AVG({col}) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '3 month' PRECEDING AND CURRENT ROW)")
        vdf.eval(name = f"LAG_{col}_sma_3M", expr = f"LAG({col}_sma_3M, 1, Null) OVER(PARTITION BY symbol ORDER BY ts)")

        vdf.eval(name = f"{col}_sma_6M", expr = f"AVG({col}) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '6 month' PRECEDING AND CURRENT ROW)")
        vdf.eval(name = f"LAG_{col}_sma_6M", expr = f"LAG({col}_sma_6M, 1, Null) OVER(PARTITION BY symbol ORDER BY ts)")

        vdf.eval(name = f"{col}_sma_1Y", expr = f"AVG({col}) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '1 year' PRECEDING AND CURRENT ROW)")
        vdf.eval(name = f"LAG_{col}_sma_1Y", expr = f"LAG({col}_sma_1Y, 1, Null) OVER(PARTITION BY symbol ORDER BY ts)")
        
        

    # TODO LAG -1 on other variables columns

def createTrainingData(target_symbol):
    print("Preparing the training data...")
    input_columns = ["open", "close", "high", "low", "volume"]

    input_colums_query_ts = ','.join([f" TS_FIRST_VALUE(\"{c}\", 'linear') as \"{c}\"" for c in input_columns])
    vdf = vdf_from_relation(f"(SELECT slice_time as ts, symbol,{input_colums_query_ts} FROM {input_table} TIMESERIES slice_time AS '1 day' OVER(PARTITION by symbol ORDER BY ts)) gapfilled")
    
    vdf.filter(conditions = [f"symbol = '{target_symbol}.SA'"])
    vdf.fillna(numeric_only = True)

    vdf.sort({"ts": "desc"})
    print(vdf)
    vdf_existing_cols = [str(c).replace('"','') for c in vdf.get_columns()]
    input_columns = [c for c in input_columns if c in vdf_existing_cols]

    # create all needed lag columns for weeks and months analysis
    print(f"Generatig variables...")
    generateVariables(vdf)

    # normalize only model columns
    model_columns = vdf.get_columns(["ts", "symbol"]+input_columns)
    model_columns = [str(c).replace('"','') for c in model_columns]
    model_columns = [c for c in model_columns if c.startswith("LAG")]
    vdf.normalize(columns = model_columns, method = "zscore")
    vdf_final_columns = ["ts", "symbol"]+input_columns+model_columns
    # save vdf to database, as a table
    new_vdf_relation = f"{input_table}_{target_symbol}_TRAINING"
    #drop_table(new_vdf_relation, print_info=False, raise_error=False)
    dropTable(new_vdf_relation)
    vdf.to_db(new_vdf_relation, usecols=vdf_final_columns, relation_type="table", inplace=True)
    print(f"Dataframe saved as: {new_vdf_relation}")

    return vdf, model_columns
        


## PROCESS ALL TARGET SYMBOLS
def createSimulationData(target_symbol, inpt_table=input_table):
    print("Preparing the simulation data...")

    input_colums_query_ts = ','.join([f" TS_FIRST_VALUE(\"{c}\", 'linear') as \"{c}\"" for c in input_columns])
    input_colums_query = ','.join([f" \"{c}\"" for c in input_columns])
    input_table_query = f"(SELECT ts, symbol,{input_colums_query} FROM {inpt_table}) input_table"
 
    #next_day = f"{next_business_day()} 00:00:00"
    from_day = getDbLastTimestamp(target_symbol, inpt_table)
    next_day = next_business_day(from_day.date())
    print(f"next_business_day: {next_day}")
    
    null_colums_query = ','.join([f"Null as \"{c}\"" for c in input_columns])
    input_table_query = f"( (SELECT ts, symbol,{input_colums_query} FROM {inpt_table} WHERE symbol = '{target_symbol}.SA' and close is not null) UNION ALL (SELECT '{next_day}'::TIMESTAMP as ts, '{target_symbol}.SA' as symbol,{null_colums_query})  ) input_table"
    vdf = vdf_from_relation(f"(SELECT slice_time as ts, symbol,{input_colums_query_ts} FROM {input_table_query} TIMESERIES slice_time AS '1 day' OVER(PARTITION by symbol ORDER BY ts)) gapfilled")
    
    vdf.filter(conditions = [f"symbol = '{target_symbol}.SA'"])
    vdf.sort({"ts": "desc"})

    vdf_existing_cols = [str(c).replace('"','') for c in vdf.get_columns()]
    _input_columns = [c for c in input_columns if c in vdf_existing_cols]

    # create all needed lag columns for weeks and months analysis
    generateVariables(vdf)
    
    # normalize only model columns
    model_columns = vdf.get_columns(["ts", "symbol"]+_input_columns)
    model_columns = [str(c).replace('"','') for c in model_columns]
    model_columns = [c for c in model_columns if c.startswith("LAG")]
    vdf.normalize(columns = model_columns, method = "zscore")

    # save vdf to database, as a table, on the first iteration
    new_vdf_relation = f"{input_table}_{target_symbol}_SIMULATION"
    if inpt_table==input_table:
        dropTable(new_vdf_relation)
        vdf.to_db(new_vdf_relation, usecols=["ts", "symbol"]+_input_columns, relation_type="table", inplace=False)
    
    # save as view 
    new_vdf_relation_view = f"{new_vdf_relation}_VIEW"
    dropView(new_vdf_relation_view)
    vdf.to_db(new_vdf_relation_view, usecols=["ts", "symbol"]+model_columns, relation_type="view", inplace=True)

    return vdf, from_day, next_day, new_vdf_relation

def trainSymbolModels(target_symbol):
    ## =========================================== ###
    output_models = {}

    print("============ TRAINING =============================")
    training_vdf, model_columns = createTrainingData(target_symbol)

    print("==================================================")
    print("Analyzing variable correlations...")
    correlation_threashold = (0.3, 0.998)

    ## https://matplotlib.org/3.1.0/tutorials/colors/colormaps.html
    for out_col in output_columns:
        model_name = f"public.LR_{target_symbol}_{out_col}"
        
        # filtrar model_columns baseado no valor das correlations
        #vdf.corr(columns = model_columns, focus=out_col, cmap="RdYlGn")
        all_correlations = training_vdf.corr(columns = model_columns, focus=out_col, show=False).values
        best_correlations = [str(c).replace('"','') for i, c in enumerate(all_correlations.get('index')) if all_correlations.get(f'"{out_col}"')[i] > correlation_threashold[0] and all_correlations.get(f'"{out_col}"')[i] < correlation_threashold[1] ]
        _model_columns = [c for c in model_columns if c != out_col and c in best_correlations]
        
        if len(_model_columns) > 0:
            print(f"Creating model: {model_name} [{len(_model_columns)} cols]")
            model = LinearRegression(model_name)
            try: model.drop()
            except: print("Training new Model...")
            model.fit(training_vdf.current_relation(), _model_columns, out_col)
            print(f"Model '{out_col}' Score: {model.score(method='r2')}")
            output_models[out_col] = model
        else:
            print(f"No column relevant for model: {model_name}!")
    
    return output_models


def simulateSymbolData(target_symbol, output_models, max_points=1):
    print("============ SIMULATING ==========================")

    if len(output_models) > 0:
        #next_day = None
        from_day = None
        simulation_vdf = None
        simul_vdf_relation = None
        for _i in range(max_points):
            if simul_vdf_relation == None: 
                simulation_vdf, from_day, _, simul_vdf_relation = createSimulationData(target_symbol)
            else: 
                simulation_vdf, from_day, _, simul_vdf_relation = createSimulationData(target_symbol, simul_vdf_relation)

            # filter future period to simulate
            simulation_vdf.filter(conditions = [f"ts >= '{from_day}'::TIMESTAMP"])

            # SIMULATE NEXT DATA POINT
            outpt_columns = [c for c in output_columns if output_models[c] != None]
            for out_col in outpt_columns:
                output_models[out_col].predict(simulation_vdf, name = f"pred_{out_col}")

            # post simulation processing using the datapoints
            if all(x in outpt_columns for x in ["volume"]):
                simulation_vdf.eval(name = f"volume", expr = f"pred_volume")
            if all(x in outpt_columns for x in ["close"]):
                simulation_vdf.eval(name = f"close", expr = f"pred_close")
            if all(x in outpt_columns for x in ["open", "close"]):
                simulation_vdf.eval(name = f"open", expr = f"((LAG(pred_close, 1, Null) OVER(PARTITION BY symbol ORDER BY ts)) + pred_open)*0.5")
            if all(x in outpt_columns for x in ["open", "close", "high", "low"]): 
                simulation_vdf.eval(name = f"high", expr = f"apply_max(ARRAY[pred_open,pred_close,pred_high,pred_low])")
                simulation_vdf.eval(name = f"low",  expr = f"apply_min(ARRAY[pred_open,pred_close,pred_high,pred_low])")

            # get only this single simulated point
            simulation_vdf.filter(conditions = [f"\"{c}\" is not Null" for c in outpt_columns])

            # save vdf to database, as a table
            simul_res_vdf_relation = f"{input_table}_{target_symbol}_SIMULATION_RESULT"
            dropTable(simul_res_vdf_relation)
            simulation_vdf.to_db(simul_res_vdf_relation, usecols=["ts", "symbol"]+outpt_columns, relation_type="table", inplace=True)
            print(simulation_vdf)
            
            print(f"Merging {simul_res_vdf_relation} into {simul_vdf_relation}")
            mergeSimulationData(simul_vdf_relation, simul_res_vdf_relation)
    else:
        print("no models found!")


runProcess()



# %%

