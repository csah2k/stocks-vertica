#%%
# pip3 install vertica-python verticapy holidays
import time
import datetime
import holidays
import vertica_python
import verticapy
import concurrent.futures
from verticapy import *
from verticapy.toolbox import *
from verticapy.connections.connect import *
from verticapy.learn.linear_model import LinearRegression
from pandas_datareader import data as web 
from datetime import datetime as dt
from finnhub import Client

finnhub_client = Client(api_key="bqu00jvrh5rb3gqqf9q0")
ONE_DAY = datetime.timedelta(days=1)
HOLIDAYS_BR = holidays.BR()

conn_info = {'host': '127.0.0.1',
            'port': 5433,
            'user': 'dbadmin',
            'password': 'stocks',
            'database': 'stocks',
            'use_prepared_statements': False}

db_schema = "stocks"
input_table = "daily_prices"
input_columns = ["open", "close", "high", "low", "volume"]
output_columns = ["open", "close", "high", "low", "volume"]

new_auto_connection(conn_info, method = "vertica_python", name = "VerticaDSN")
change_auto_connection("VerticaDSN")

vertica_connection = vertica_python.connect(**conn_info)
vert_cur = vertica_connection.cursor()

executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)

def runProcess():
    
    # TODO BUSCAR DO DB
    symbols = [ 'ABEV3.SA',
                'B3SA3.SA',
                'BBAS3.SA',
                'BBDC4.SA',
                'BBSE3.SA',
                'BOVA11.SA',
                'BRFS3.SA',
                'BRKM5.SA',
                'BRML3.SA',
                'CCRO3.SA',
                'CIEL3.SA',
                'CMIG4.SA',
                'COGN3.SA',
                'CSAN3.SA',
                'CSNA3.SA',
                'CYRE3.SA',
                'ECOR3.SA',
                'EGIE3.SA',
                'ELET3.SA',
                'EMBR3.SA',
                'EQTL3.SA',
                'GGBR4.SA',
                'GOAU4.SA',
                'HYPE3.SA',
                'IRBR3.SA',
                'ITSA4.SA',
                'ITUB4.SA',
                'JBSS3.SA',
                'LAME4.SA',
                'LREN3.SA',
                'MGLU3.SA',
                'MRFG3.SA',
                'NTCO3.SA',
                'CVCB3.SA',
                'PETR4.SA',
                'QUAL3.SA',
                'RADL3.SA',
                'RAIL3.SA',
                'RENT3.SA',
                'SBSP3.SA',
                'SUZB3.SA',
                'TAEE11.SA',
                'USIM5.SA',
                'VALE3.SA',
                'VIVT4.SA',
                'VVAR3.SA',
                'WEGE3.SA',
                'YDUQ3.SA']

    '''
    symbols = [ 'CVCB3.SA',
                'PETR4.SA',
                'ITUB4.SA',
                'BRML3.SA']
    '''


    ## MULTI_THREAD ##
    '''
    open_threads = []
    for target_symbol in symbols:
        open_threads.append(executor.submit(processSymbol, target_symbol))
        time.sleep(10)
    for open_thread in open_threads:
        open_thread.result()
    '''
    
    for target_symbol in symbols:
        processSymbol(target_symbol)
        #symbol_models = {}
        #for c in output_columns:
        #    m = LinearRegression(f"\"{db_schema}\".\"LR_{symbolToTable(target_symbol)}_{c}\"", solver="CGD")
        #    symbol_models[c] = m

        #simulateSymbolData(target_symbol, trainSymbolModels(target_symbol), 10)
        #except:
        #    print("Error")
        #input("PRESS ANY KEY TO CONTINUE!")

    print("========= ALL SYMBOLS FINISHED ========")
    

def processSymbol(target_symbol):
    ### LOAD DATA ###
    loadHistData(target_symbol)
    ### TRAIN MODELS  ###   
    models =  trainSymbolModels(target_symbol)
    ### SIMULATE DATA ###
    simulateSymbolData(target_symbol, models, 5)
    print(f"========= SYMBOL {target_symbol} FINISHED ========")

def getRelation(table=input_table, schema=db_schema, table_only=False):
    table_search = re.search(r"(\"?(\w+)\"?\.)?\"?(\w+)\"?", table, re.IGNORECASE)
    if table_search:
        table = table_search.group(3)
    if table_only: return table
    else: return f"\"{schema}\".\"{table}\"" 

def next_business_day(ref_date=datetime.date.today()):
    next_day = ref_date + ONE_DAY
    while next_day.weekday() in holidays.WEEKEND or next_day in HOLIDAYS_BR:
        next_day += ONE_DAY
    return next_day

def getDbLastTimestamp(symbol, table_name="daily_prices", column_name="ts"):    
    return vert_cur.execute(f"SELECT COALESCE(MAX(\"{column_name}\"), '1990-01-01') as \"{column_name}\" FROM {getRelation(table_name)} WHERE symbol = '{symbol}' and close is not null;").fetchone()[0]

def mergeSimulationData(simulation_table, simulation_result_table):
    return vert_cur.execute(f"MERGE INTO {getRelation(simulation_table)} sim USING {getRelation(simulation_result_table)} res ON sim.ts = res.ts WHEN MATCHED THEN UPDATE SET open = res.open, close = res.close, high = res.high, low = res.low, volume = res.volume WHEN NOT MATCHED THEN INSERT (ts, symbol, open, close, high, low, volume) VALUES (res.ts, res.symbol, res.open, res.close, res.high, res.low, res.volume);")

def dropTable(table_name, cascade=True):
    sql = f"DROP TABLE IF EXISTS {getRelation(table_name)}"
    if cascade: sql = f"{sql} CASCADE;"
    return vert_cur.execute(sql)

def dropView(view_name):
    sql = f"DROP VIEW IF EXISTS {getRelation(view_name)};"
    return vert_cur.execute(sql)

def symbolToTable(symbol):
    return re.sub(r'[^A-Z09]', '', symbol).lower().strip()

def loadHistData(symbol):
    print(f"Loading {symbol} data from yahoo...")
    df = web.DataReader(
        symbol,
        'yahoo',
        getDbLastTimestamp(symbol),
        dt.now()
    ).reset_index()

    print(df)
    table_name_stg = getRelation(f"stock_data_{symbolToTable(symbol)}")
    dropTable(table_name_stg)
    pandas_to_vertica(df, getRelation(table_name_stg, table_only=True), schema=db_schema)

    query = f"INSERT INTO \"{db_schema}\".\"{input_table}\" SELECT \"Date\"::TIMESTAMP as \"ts\", '{symbol}' as \"symbol\", \"Open\" as \"open\", \"High\" as \"high\", \"Low\" as \"low\", \"Close\" as \"close\", \"Volume\" as \"volume\" FROM {table_name_stg} WHERE \"Volume\" > 0 AND \"Close\" > 0 AND \"Date\" > (select COALESCE(MAX(ts), '1990-01-01') from {getRelation(input_table)} where symbol = '{symbol}') "
    print(query)
    vert_cur.execute(query).fetchone()
    dropTable(table_name_stg)

    return input_table

def generateTempName(target_symbol):
    tmp = f"{input_table}_{symbolToTable(target_symbol)}_{random.randint(100000, 999999)}"
    return tmp

def getModelCols(vdf):
    model_columns = vdf.get_columns()
    model_columns = [str(c).replace('"','') for c in model_columns]
    model_columns = [c for c in model_columns if c.startswith("LAG") or c in ["ts", "symbol"]+input_columns ]
    return model_columns

def materializeVdf(tmp, target_symbol, vdf, usecols=[]):
    prev_tmp = tmp
    tmp = getRelation(generateTempName(target_symbol))
    dropTable(tmp)
    vdf.to_db(tmp, usecols=usecols, relation_type="table", inplace=True)
    dropTable(prev_tmp)
    return tmp

def generateVariables(tmp, target_symbol, vdf):

    # https://www.investopedia.com/terms/m/macd.asp
    # https://www.investopedia.com/terms/m/movingaverage.asp
    # https://www.mssqltips.com/sqlservertip/5441/using-tsql-to-detect-golden-crosses-and-death-crosses/
    
    # DAY AVERAGE
    #vdf.eval(name = f"dayavg_close", expr = f"apply_avg(ARRAY[open,close,high,low])")

    for col in input_columns: # +["dayavg_close"]:
        # LAG D-1
        #vdf.eval(name = f"{col}_D1_N", expr = f"LAG({col}, 1, Null) OVER(PARTITION BY symbol ORDER BY ts)")
        #vdf.eval(name = f"{col}_D1_1", expr = f"LAG({col}, 1, 1) OVER(PARTITION BY symbol ORDER BY ts)")
 
        # https://www.fmlabs.com/reference/default.htm?url=SimpleMA.htm
        vdf.eval(name = f"{col}_sma_1W", expr = f"AVG({col}) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '1 week' PRECEDING AND CURRENT ROW)")
        vdf.eval(name = f"{col}_sma_1M", expr = f"AVG({col}) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '1 month' PRECEDING AND CURRENT ROW)")
        vdf.eval(name = f"{col}_sma_3M", expr = f"AVG({col}) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '3 month' PRECEDING AND CURRENT ROW)")
        vdf.eval(name = f"{col}_sma_6M", expr = f"AVG({col}) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '6 month' PRECEDING AND CURRENT ROW)") 
        vdf.eval(name = f"{col}_sma_9M", expr = f"AVG({col}) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '9 month' PRECEDING AND CURRENT ROW)")
        vdf.eval(name = f"{col}_sma_1Y", expr = f"AVG({col}) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '1 year' PRECEDING AND CURRENT ROW)")
        
        
        # https://www.fmlabs.com/reference/default.htm?url=ExpMA.htm
        #vdf.eval(name = f"{col}_vshort_ema", expr = f"EXPONENTIAL_MOVING_AVERAGE({col}, 0.2) OVER (PARTITION BY symbol ORDER BY ts)")
        vdf.eval(name = f"{col}_short_ema", expr = f"EXPONENTIAL_MOVING_AVERAGE({col}, 0.15) OVER (PARTITION BY symbol ORDER BY ts)")
        vdf.eval(name = f"{col}_long_ema", expr = f"EXPONENTIAL_MOVING_AVERAGE({col}, 0.075) OVER (PARTITION BY symbol ORDER BY ts)")
        vdf.eval(name = f"{col}_vlong_ema", expr = f"EXPONENTIAL_MOVING_AVERAGE({col}, 0.0375) OVER (PARTITION BY symbol ORDER BY ts)")
        
        # https://www.fmlabs.com/reference/default.htm?url=MACD.ht
        #vdf.eval(name = f"{col}_MACD", expr = f"{col}_short_ema - {col}_long_ema")



    #  https://www.investopedia.com/terms/p/pricerateofchange.asp
    '''
    vdf.eval(name = "roc", expr = "(close - close_D1_N )/ close_D1_1 * 100")
    vdf.eval(name = "roc_long_ema", expr = "EXPONENTIAL_MOVING_AVERAGE(roc, 0.075) OVER (PARTITION BY symbol ORDER BY ts)")
    vdf.eval(name = "roc_short_ema", expr = "EXPONENTIAL_MOVING_AVERAGE(roc, 0.15) OVER (PARTITION BY symbol ORDER BY ts)")
    '''

    # https://www.investopedia.com/terms/v/vwap.asp
    '''
    vdf.eval(name = "PV", expr = "volume * ((high + low + close) / 3)")
    vdf.eval(name = "VWAP", expr = "PV / volume")
    '''

    # https://www.fmlabs.com/reference/default.htm?url=RSI.htm
    '''
    vdf.eval(name = "_up", expr = "CASE WHEN close > close_D1_1 THEN close - close_D1_1 ELSE 0 END")
    vdf.eval(name = "_dn", expr = "CASE WHEN close > close_D1_1 THEN 0 ELSE close_D1_1 - close END")

    vdf.eval(name = "_upavg_1M", expr = "AVG(_up) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '1 month' PRECEDING AND CURRENT ROW)")
    vdf.eval(name = "_dnavg_1M", expr = "AVG(_dn) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '1 month' PRECEDING AND CURRENT ROW)")
    vdf.eval(name = "RMI_1M", expr = "100 * ( _upavg_1M / ( _upavg_1M - _dnavg_1M ))")
    
    vdf.eval(name = "_upavg_3M", expr = "AVG(_up) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '3 month' PRECEDING AND CURRENT ROW)")
    vdf.eval(name = "_dnavg_3M", expr = "AVG(_dn) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '3 month' PRECEDING AND CURRENT ROW)")
    vdf.eval(name = "RMI_3M", expr = "100 * ( _upavg_3M / ( _upavg_3M - _dnavg_3M ))")
    '''

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

    # ===================== LAGS =====================
    for col in input_columns:
        vdf.eval(name = f"LAG_{col}_sma_1W", expr = f"LAG({col}_sma_1W, 1, 0) OVER(PARTITION BY symbol ORDER BY ts)")
        vdf.eval(name = f"LAG_{col}_sma_1M", expr = f"LAG({col}_sma_1M, 1, 0) OVER(PARTITION BY symbol ORDER BY ts)")
        vdf.eval(name = f"LAG_{col}_sma_3M", expr = f"LAG({col}_sma_3M, 1, 0) OVER(PARTITION BY symbol ORDER BY ts)")
        vdf.eval(name = f"LAG_{col}_sma_6M", expr = f"LAG({col}_sma_6M, 1, 0) OVER(PARTITION BY symbol ORDER BY ts)")
        vdf.eval(name = f"LAG_{col}_sma_1Y", expr = f"LAG({col}_sma_1Y, 1, 0) OVER(PARTITION BY symbol ORDER BY ts)")
        #vdf.eval(name = f"LAG_{col}_vshort_ema", expr = f"LAG({col}_vshort_ema, 1, Null) OVER(PARTITION BY symbol ORDER BY ts)")
        vdf.eval(name = f"LAG_{col}_short_ema", expr = f"LAG({col}_short_ema, 1, Null) OVER(PARTITION BY symbol ORDER BY ts)")
        vdf.eval(name = f"LAG_{col}_long_ema", expr = f"LAG({col}_long_ema, 1, Null) OVER(PARTITION BY symbol ORDER BY ts)")
        vdf.eval(name = f"LAG_{col}_vlong_ema", expr = f"LAG({col}_vlong_ema, 1, Null) OVER(PARTITION BY symbol ORDER BY ts)")        
        #vdf.eval(name = f"LAG_{col}_MACD", expr = f"LAG({col}_MACD, 1, Null) OVER(PARTITION BY symbol ORDER BY ts)")

    '''
    vdf.eval(name = "LAG_STOCH_1M_close", expr = "LAG(STOCH_1M, 1, 0) OVER(PARTITION BY symbol ORDER BY ts)")
    vdf.eval(name = "LAG_STOCH_3M_close", expr = "LAG(STOCH_3M , 1, 0) OVER(PARTITION BY symbol ORDER BY ts)")
    vdf.eval(name = "LAG_roc_long_ema_close", expr = "LAG(roc_long_ema, 1, 0) OVER(PARTITION BY symbol ORDER BY ts)")
    vdf.eval(name = "LAG_roc_short_ema_close", expr = "LAG(roc_short_ema, 1, 0) OVER(PARTITION BY symbol ORDER BY ts)")
    vdf.eval(name = "LAG_VWAP_close", expr = "LAG(VWAP, 1, 0) OVER(PARTITION BY symbol ORDER BY ts)")
    vdf.eval(name = f"LAG_RMI_1M_close", expr = f"LAG(RMI_1M, 1, 0) OVER(PARTITION BY symbol ORDER BY ts)")
    vdf.eval(name = f"LAG_RMI_3M_close", expr = f"LAG(RMI_3M, 1, 0) OVER(PARTITION BY symbol ORDER BY ts)")
    '''

    # https://www.fmlabs.com/reference/default.htm?url=DI.htm
    #vdf.eval(name = f"_delta_high", expr = f"LAG(high, 1, Null) OVER(PARTITION BY symbol ORDER BY ts) - high")
    #vdf.eval(name = f"_delta_low", expr = f"low - LAG(low, 1, Null) OVER(PARTITION BY symbol ORDER BY ts)")
    #vdf.eval(name = f"_plus_dm", expr = f"CASE WHEN ( _delta_high < 0 AND _delta_low < 0 ) OR _delta_high = _delta_low THEN 0 WHEN _delta_high > _delta_low THEN _delta_high ELSE 0 END")
    #vdf.eval(name = f"_minus_dm", expr = f"CASE WHEN ( _delta_high < 0 AND _delta_low < 0 ) OR _delta_high = _delta_low THEN 0 WHEN _delta_high > _delta_low THEN 0 ELSE _delta_low END")
    
    #vdf.eval(name = f"_plus_dm_sum", expr = f"LAG(RMI_3M, 1, Null) OVER(PARTITION BY symbol ORDER BY ts) ")
    #vdf.eval(name = f"_minus_dm_sum", expr = f"")
    
    
    # https://www.fmlabs.com/reference/default.htm?url=DX.htm

    # https://www.fmlabs.com/reference/default.htm?url=ADX.htm    

    return tmp

    
def createTrainingData(target_symbol):
    print("Preparing the training data...")
    input_columns = ["close", "open", "high", "low", "volume"]
    
    ## MATERIALIZE
    tmp = getRelation(generateTempName(target_symbol))
    dropTable(tmp)
    input_colums_query_ts = ','.join([f" TS_FIRST_VALUE(\"{c}\", 'linear') as \"{c}\"" for c in input_columns])
    query_create_tmp = f"CREATE TABLE {tmp} as SELECT \"slice_time\" as \"ts\", \"symbol\",{input_colums_query_ts} FROM {getRelation(input_table)} WHERE \"symbol\" = '{target_symbol}' TIMESERIES \"slice_time\" AS '1 day' OVER(PARTITION by symbol ORDER BY ts);"
    vert_cur.execute(query_create_tmp)
    vdf = vDataFrame(tmp)
    
    #vdf.filter(conditions = [f"symbol = '{target_symbol}'"])    
    if vdf.empty():
        print("No enough data points!")
        return vdf, []

    vdf.fillna(numeric_only = True)
    vdf.sort({"ts": "desc"})
    print(vdf)
    vdf_existing_cols = [str(c).replace('"','') for c in vdf.get_columns()]
    input_columns = [c for c in input_columns if c in vdf_existing_cols]

    # create all needed lag columns for weeks and months analysis
    print(f"Generatig variables...")
    tmp = generateVariables(tmp, target_symbol, vdf)

    # normalize only model columns
    model_columns = vdf.get_columns(["ts", "symbol"]+input_columns)
    model_columns = [str(c).replace('"','') for c in model_columns]
    model_columns = [c for c in model_columns if c.startswith("LAG")]
    #vdf.normalize(columns = model_columns, method = "zscore")
    vdf_final_columns = ["ts", "symbol"]+input_columns+model_columns

    # save vdf to database, as a table
    new_vdf_relation = getRelation(f"{input_table}_{symbolToTable(target_symbol)}_training")
    dropTable(new_vdf_relation)
    vdf.to_db(new_vdf_relation, usecols=vdf_final_columns, relation_type="table", inplace=True)
    print(f"Dataframe saved as: {new_vdf_relation}")
    dropTable(tmp)

    return vdf, model_columns
        

## PROCESS ALL TARGET SYMBOLS
def createSimulationData(target_symbol, inpt_table=input_table, first_time=False):
    print("Preparing the simulation data...")

    ##  MATERIALIZE
    tmp = getRelation(generateTempName(target_symbol))
    dropTable(tmp)
    input_colums_query_ts = ','.join([f" TS_FIRST_VALUE(\"{c}\", 'linear') as \"{c}\"" for c in input_columns])
    input_colums_query = ','.join([f" \"{c}\"" for c in input_columns])

    # generate next dat point point
    from_day = getDbLastTimestamp(target_symbol, inpt_table)
    next_day = next_business_day(from_day.date())
    print(f"next_business_day: {next_day}")

    inpt_table = getRelation(inpt_table)
    null_colums_query = ','.join([f" Null as \"{c}\"" for c in input_columns])
    input_table_query = f"( (SELECT ts, symbol,{input_colums_query} FROM {inpt_table} WHERE symbol = '{target_symbol}' and close is not null) UNION ALL (SELECT '{next_day}'::TIMESTAMP as ts, '{target_symbol}' as symbol,{null_colums_query})  ) \"input_table\""
    query_create_tmp = f"CREATE TABLE {tmp} as SELECT slice_time as ts, symbol,{input_colums_query_ts} FROM {input_table_query} TIMESERIES slice_time AS '1 day' OVER(PARTITION by symbol ORDER BY ts)"
    vert_cur.execute(query_create_tmp)
    vdf = vDataFrame(tmp)

    #vdf.filter(conditions = [f"symbol = '{target_symbol}'"])
    vdf.sort({"ts": "desc"})
    print(vdf)

    vdf_existing_cols = [str(c).replace('"','') for c in vdf.get_columns()]
    _input_columns = [c for c in input_columns if c in vdf_existing_cols]

    # create all needed lag columns for weeks and months analysis
    tmp = generateVariables(tmp, target_symbol, vdf)
    
    # normalize only model columns
    model_columns = vdf.get_columns(["ts", "symbol"]+_input_columns)
    model_columns = [str(c).replace('"','') for c in model_columns]
    model_columns = [c for c in model_columns if c.startswith("LAG")]
    #vdf.normalize(columns = model_columns, method = "zscore")

    # save vdf to database, as a table, on the first iteration
    new_vdf_relation = getRelation(f"{input_table}_{symbolToTable(target_symbol)}_simulation")
    if first_time:
        dropTable(new_vdf_relation)
        vdf.to_db(new_vdf_relation, usecols=["ts", "symbol"]+_input_columns, relation_type="table", inplace=False)
    
    # save as view 
    new_vdf_relation_view = getRelation(f"{input_table}_{symbolToTable(target_symbol)}_view")
    dropView(new_vdf_relation_view)
    vdf.to_db(new_vdf_relation_view, usecols=["ts", "symbol"]+model_columns, relation_type="view", inplace=True)

    # TODO create projections in the new_vdf_relation
    #dropTable(tmp) 

    return vdf, from_day, next_day, new_vdf_relation

def trainSymbolModels(target_symbol):
    ## =========================================== ###
    output_models = {}
    max_iterations = 100
    correlation_threashold = (0.300, 0.999)

    print("============ TRAINING =============================")
    training_vdf, model_columns = createTrainingData(target_symbol)

    print("==================================================")
    print("Analyzing variable correlations...")

    ## https://matplotlib.org/3.1.0/tutorials/colors/colormaps.html
    for out_col in output_columns:
        model_name = f"\"{db_schema}\".\"LR_{symbolToTable(target_symbol)}_{out_col}\""
        
        # filtrar model_columns baseado no valor das correlations
        #vdf.corr(columns = model_columns, focus=out_col, cmap="RdYlGn")
        show_graph = False #(out_col == "close")
        all_correlations = training_vdf.corr(columns = model_columns, focus=out_col, show=show_graph).values
        best_correlations = [str(c).replace('"','') for i, c in enumerate(all_correlations.get('index')) if all_correlations.get(f'"{out_col}"')[i] > correlation_threashold[0] and all_correlations.get(f'"{out_col}"')[i] < correlation_threashold[1] ]
        _model_columns = [c for c in model_columns if c != out_col and c.find(out_col)>0 and c in best_correlations ]

        #print(f"out_col: {out_col}")
        if show_graph:
            print(f"model_columns: {_model_columns}")
            input("PRESS ANY KEY TO CONTINUE!")
        if (out_col == "close"): 
            print(f"model_columns: {_model_columns}")
        
        if len(_model_columns) > 0:
            print(f"Creating model: {model_name} [{len(_model_columns)} cols]")
            model = LinearRegression(model_name, max_iter=max_iterations, solver='Newton')
            try: model.drop()
            except: print("Training new Model...")
            try: model.fit(training_vdf.current_relation(), _model_columns, out_col)
            except:
                print("Error using Newton solver, trying BFGS...")
                model = LinearRegression(model_name, max_iter=max_iterations, solver='BFGS')
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
                simulation_vdf, from_day, _, simul_vdf_relation = createSimulationData(target_symbol, first_time=True)
            else: 
                simulation_vdf, from_day, _, simul_vdf_relation = createSimulationData(target_symbol, inpt_table=simul_vdf_relation)

           

            # SIMULATE NEXT DATA POINT
            outpt_columns = [c for c in output_columns if output_models[c] != None]
            for out_col in outpt_columns:
                output_models[out_col].predict(simulation_vdf, name = f"pred_{out_col}")

            # post simulation processing using the datapoints
            if all(x in outpt_columns for x in ["volume"]):
                simulation_vdf.eval(name = f"volume", expr = f"ABS(pred_volume)::INTEGER")
            if all(x in outpt_columns for x in ["close"]):
                simulation_vdf.eval(name = f"close", expr = f"ABS(pred_close)")
            if all(x in outpt_columns for x in ["open", "close"]):
                simulation_vdf.eval(name = f"open", expr = f"( LAG(close, 1, 1) OVER(PARTITION BY symbol ORDER BY ts) * 0.8)  + ( ABS(pred_open) * 0.2 )")
            if all(x in outpt_columns for x in ["open", "close", "high", "low"]): 
                simulation_vdf.eval(name = f"a_high", expr = f"apply_max(ARRAY[ open, close, ABS(pred_high), ABS(pred_low) ])")
                simulation_vdf.eval(name = f"a_low",  expr = f"apply_min(ARRAY[ open, close, ABS(pred_high), ABS(pred_low) ])")
                simulation_vdf.eval(name = f"high", expr = f"( ( LAG(a_high, 1, 1) OVER(PARTITION BY symbol ORDER BY ts) * 0.8) + ( a_high * 0.2 ) ) * 1.01")
                simulation_vdf.eval(name = f"low", expr = f"( ( LAG(a_low, 1, 0) OVER(PARTITION BY symbol ORDER BY ts) * 0.8) + ( a_low * 0.2 ) ) * 0.99")
                #simulation_vdf.eval(name = f"high", expr = f"apply_avg(ARRAY[b_high, close]) * 1.01")
                #simulation_vdf.eval(name = f"low", expr = f"apply_avg(ARRAY[b_low, close]) * 0.99 ")


            # get only this single simulated point
            simulation_vdf.filter(conditions = [f"\"{c}\" is not Null" for c in outpt_columns]+[f"ts >= '{from_day}'::TIMESTAMP"])

            # save vdf to database, as a table
            simul_res_vdf_relation = getRelation(f"{input_table}_{symbolToTable(target_symbol)}_prediction")
            dropTable(simul_res_vdf_relation)
            simulation_vdf.to_db(simul_res_vdf_relation, usecols=["ts", "symbol"]+outpt_columns, relation_type="table", inplace=True)
            print(simulation_vdf)
            
            print(f"Merging {simul_res_vdf_relation} into {simul_vdf_relation}")
            mergeSimulationData(simul_vdf_relation, simul_res_vdf_relation)

        # FINSIHED
        return from_day
        
    else:
        print("no models found!")
        return None


runProcess()



# %%

