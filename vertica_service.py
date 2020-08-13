# pip3 install vertica-python verticapy holidays
import time
import logging
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

logfile = 'vertica_service.log'

conn_info = {'host': '192.168.1.230',
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
#vert_cur = vertica_connection.cursor()

executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
logging.basicConfig(format='%(asctime)s (%(threadName)s) %(levelname)s - %(message)s', level=logging.INFO, handlers=[logging.FileHandler(logfile, 'w', 'utf-8')])



def runProcess(load_data=False, train_models=True, simulate_data=True):
    logging.info("========= STARTING PROCESS ========")

    ### LOAD DATA ###
    if load_data:
        for target_symbol in listSymbols():
            loadHistData(target_symbol)
        # Table Maintenance
        with vertica_connection.cursor() as vert_cur:
            data_table = getRelation()
            vert_cur.execute(f"select do_tm_task('mergeout', '{data_table}');")
            vert_cur.execute(f"select analyze_statistics('{data_table}', 100);")

    ### TRAIN & SIMULATE DATA ###
    if simulate_data:
        models = trainSymbolModels()
        # listSymbols(input_table):
        for target_symbol in ['ABEV3.SA']:
            processSymbol(target_symbol, models)
    else:
        ### ONLY TRAIN MODELS ###
        if train_models:
            models = trainSymbolModels()
        
    #for target_symbol in listSymbols():
        #processSymbol(target_symbol)
        #symbol_models = {}
        #for c in output_columns:
        #    m = LinearRegression(f"\"{db_schema}\".\"LR_{symbolToTable(target_symbol)}_{c}\"", solver="CGD")
        #    symbol_models[c] = m

        #simulateSymbolData(target_symbol, trainSymbolModels(target_symbol), 10)
        #except:
        #    print("Error")
        #input("PRESS ANY KEY TO CONTINUE!")

    logging.info("========= ALL SYMBOLS FINISHED ========")
    

def processSymbol(target_symbol, models):
    logging.info(f'Processing symbol {target_symbol}')
    try:
        
        simulateSymbolData(target_symbol, models, 4)
        logging.info(f"Symbol {target_symbol} Finished!")
    except Exception as error:
        logging.error(f"ERROR IN SYMBOL: {target_symbol}")
        logging.error(error)    

def listSymbols(table_name="stock_symbols"):
    table_name = getRelation(table_name)
    logging.info(f"Listing symbols from table {table_name}")
    with vertica_connection.cursor() as vert_cur:
        return [row[0] for row in vert_cur.execute(f"SELECT DISTINCT symbol FROM {table_name} order by 1;").fetchall()]

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
    logging.info(f"Next business day: {next_day}")
    return next_day

def getDbLastTimestamp(symbol, table_name="daily_prices", column_name="ts"):
    with vertica_connection.cursor() as vert_cur: 
        return vert_cur.execute(f"SELECT COALESCE(MAX(\"{column_name}\"), '1990-01-01') as \"{column_name}\" FROM {getRelation(table_name)} WHERE symbol = '{symbol}' and close is not null;").fetchone()[0]

def mergeSimulationData(simulation_table, simulation_result_table):
    with vertica_connection.cursor() as vert_cur:
        return vert_cur.execute(f"MERGE INTO {getRelation(simulation_table)} sim USING {getRelation(simulation_result_table)} res ON sim.ts = res.ts WHEN MATCHED THEN UPDATE SET open = res.open, close = res.close, high = res.high, low = res.low, volume = res.volume WHEN NOT MATCHED THEN INSERT (ts, symbol, open, close, high, low, volume) VALUES (res.ts, res.symbol, res.open, res.close, res.high, res.low, res.volume);")

def dropTable(table_name, cascade=True):
    table_name = getRelation(table_name)
    sql = f"DROP TABLE IF EXISTS {table_name}"
    if cascade: sql = f"{sql} CASCADE;"
    with vertica_connection.cursor() as vert_cur:
        vert_cur.execute(sql)
    return table_name

def dropView(view_name):
    sql = f"DROP VIEW IF EXISTS {getRelation(view_name)};"
    with vertica_connection.cursor() as vert_cur:
        return vert_cur.execute(sql)

def symbolToTable(symbol):
    return re.sub(r'[^A-Z09]', '', symbol).lower().strip()

def loadHistData(symbol):
    logging.info(f"Loading new data from YAHOO! for {symbol}")
    df = web.DataReader(
        symbol,
        'yahoo',
        getDbLastTimestamp(symbol),
        dt.now()
    ).reset_index()

    table_name_stg = getRelation(f"stock_data_{symbolToTable(symbol)}")
    dropTable(table_name_stg)
    pandas_to_vertica(df, getRelation(table_name_stg, table_only=True), schema=db_schema)

    target_table = getRelation(input_table)
    query = f"INSERT INTO {target_table} SELECT \"Date\"::TIMESTAMP as \"ts\", '{symbol}' as \"symbol\", \"Open\" as \"open\", \"High\" as \"high\", \"Low\" as \"low\", \"Close\" as \"close\", \"Volume\" as \"volume\" FROM {table_name_stg} WHERE \"Volume\" > 0 AND \"Close\" > 0 AND \"Date\" >= (select COALESCE(MAX(ts), '1990-01-01') from {getRelation(input_table)} where symbol = '{symbol}') "
    logging.info(query)
    with vertica_connection.cursor() as vert_cur:
        vert_cur.execute(query).fetchone()
    dropTable(table_name_stg)
    logging.info(f"Finished {symbol} data loading in {target_table}")
    return input_table

def generateTempName(target_symbol):
    tmp = f"{input_table}_{symbolToTable(target_symbol)}_{random.randint(100000, 999999)}"
    return tmp

def getModelCols(vdf):
    model_columns = vdf.get_columns()
    model_columns = [str(c).replace('"','') for c in model_columns]
    model_columns = [c for c in model_columns if c.startswith("LAG") or c in ["ts", "symbol"]+input_columns ]
    return model_columns

def materializeVdf(vdf, tmp='None', target_symbol='', usecols=[]):
    prev_tmp = tmp
    tmp = dropTable(getRelation(generateTempName(target_symbol)))
    logging.info(f"Materializing temporary table {tmp}")
    vdf.to_db(tmp, usecols=usecols, relation_type="temporary", inplace=True)
    dropTable(prev_tmp)
    return tmp

def calculateColumns(vdf):

    logging.info("Calculating new columns...")
    n = 20 # Number of days in smoothing period (typically 20)
    m = 2.1  # Number of standard deviations (typically 2)

    # https://www.investopedia.com/terms/m/macd.asp
    # https://www.investopedia.com/terms/m/movingaverage.asp
    # https://www.mssqltips.com/sqlservertip/5441/using-tsql-to-detect-golden-crosses-and-death-crosses/
    
    # DAY AVERAGE
    #vdf.eval(name = f"dayavg_close", expr = f"apply_avg(ARRAY[open,close,high,low])")


    # DATE PARTS
    '''
    vdf.eval(name = "LAG_date_dow_all", expr = "LAG(DATE_PART('ISODOW', ts), 1, 1) OVER(PARTITION BY symbol ORDER BY ts)")
    vdf.eval(name = "LAG_date_woy_all", expr = "LAG(DATE_PART('ISOWEEK', ts), 1, 1) OVER(PARTITION BY symbol ORDER BY ts)")
    vdf.eval(name = "LAG_date_qtr_all", expr = "LAG(DATE_PART('QUARTER', ts), 1, 1) OVER(PARTITION BY symbol ORDER BY ts)")
    '''


    # https://www.fmlabs.com/reference/default.htm?url=RSI.htm
    vdf.eval(name = "_clse_D1_0", expr = "LAG(close, 1, 0) OVER(PARTITION BY symbol ORDER BY ts)")
    vdf.eval(name = "_up", expr = "CASE WHEN close > _clse_D1_0 THEN close - _clse_D1_0 ELSE 1 END")
    vdf.eval(name = "_dn", expr = "CASE WHEN close > _clse_D1_0 THEN 0 ELSE _clse_D1_0 - close END")
    vdf.eval(name = "_upavg", expr = f"AVG(_up) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '{n-1} days' PRECEDING AND CURRENT ROW)")
    vdf.eval(name = "_dnavg", expr = f"AVG(_dn) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '{n-1} days' PRECEDING AND CURRENT ROW)")
    vdf.eval(name = "LAG_RSI_all", expr = "ROUND(100 * ( _upavg / ( _upavg + _dnavg )), 2)")
 

    for col in input_columns:
        # LAG D-1
        #vdf.eval(name = f"{col}_D1_N", expr = f"LAG({col}, 1, Null) OVER(PARTITION BY symbol ORDER BY ts)")
        #vdf.eval(name = f"{col}_D1", expr = f"LAG({col}, 1, 1) OVER(PARTITION BY symbol ORDER BY ts)")
 
        # https://www.fmlabs.com/reference/default.htm?url=SimpleMA.htm
        vdf.eval(name = f"{col}_sma_N", expr = f"AVG({col}) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '{n} days' PRECEDING AND CURRENT ROW)")
        #vdf.eval(name = f"{col}_sma_3D", expr = f"AVG({col}) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '3 days' PRECEDING AND CURRENT ROW)")
        vdf.eval(name = f"{col}_sma_1W", expr = f"AVG({col}) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '1 week' PRECEDING AND CURRENT ROW)")
        #vdf.eval(name = f"{col}_sma_1M", expr = f"AVG({col}) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '1 month' PRECEDING AND CURRENT ROW)") 
        vdf.eval(name = f"{col}_sma_3M", expr = f"AVG({col}) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '3 month' PRECEDING AND CURRENT ROW)") 
        #vdf.eval(name = f"{col}_sma_6M", expr = f"AVG({col}) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '6 month' PRECEDING AND CURRENT ROW)") 
        #vdf.eval(name = f"{col}_sma_9M", expr = f"AVG({col}) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '9 month' PRECEDING AND CURRENT ROW)")
        #vdf.eval(name = f"{col}_sma_1Y", expr = f"AVG({col}) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '1 year' PRECEDING AND CURRENT ROW)")
        
        
        # https://www.fmlabs.com/reference/default.htm?url=ExpMA.htm
        #vdf.eval(name = f"{col}_vshort_ema", expr = f"EXPONENTIAL_MOVING_AVERAGE({col}, 0.2) OVER (PARTITION BY symbol ORDER BY ts)")
        vdf.eval(name = f"{col}_short_ema", expr = f"EXPONENTIAL_MOVING_AVERAGE({col}, 0.15) OVER (PARTITION BY symbol ORDER BY ts)")
        vdf.eval(name = f"{col}_long_ema", expr = f"EXPONENTIAL_MOVING_AVERAGE({col}, 0.075) OVER (PARTITION BY symbol ORDER BY ts)")
        #vdf.eval(name = f"{col}_vlong_ema", expr = f"EXPONENTIAL_MOVING_AVERAGE({col}, 0.0375) OVER (PARTITION BY symbol ORDER BY ts)")

        
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
        for lag in [1, 7, 14]: # LAG D-N
            vdf.eval(name = f"LAG_{col}_sma_N_D{lag}", expr = f"LAG({col}_sma_N, {lag}, Null) OVER(PARTITION BY symbol ORDER BY ts)")
            #vdf.eval(name = f"LAG_{col}_sma_3D_D{lag}", expr = f"LAG({col}_sma_3D, {lag}, Null) OVER(PARTITION BY symbol ORDER BY ts)")
            vdf.eval(name = f"LAG_{col}_sma_1W_D{lag}", expr = f"LAG({col}_sma_1W, {lag}, Null) OVER(PARTITION BY symbol ORDER BY ts)")
            #vdf.eval(name = f"LAG_{col}_sma_1M_D{lag}", expr = f"LAG({col}_sma_1M, {lag}, 0) OVER(PARTITION BY symbol ORDER BY ts)")
            vdf.eval(name = f"LAG_{col}_sma_3M_D{lag}", expr = f"LAG({col}_sma_3M, {lag}, Null) OVER(PARTITION BY symbol ORDER BY ts)")
            #vdf.eval(name = f"LAG_{col}_sma_6M_D{lag}", expr = f"LAG({col}_sma_6M, {lag}, Null) OVER(PARTITION BY symbol ORDER BY ts)")
            #vdf.eval(name = f"LAG_{col}_sma_9M_D{lag}", expr = f"LAG({col}_sma_9M, {lag}, 0) OVER(PARTITION BY symbol ORDER BY ts)")
            #vdf.eval(name = f"LAG_{col}_sma_1Y_D{lag}", expr = f"LAG({col}_sma_1Y, {lag}, 0) OVER(PARTITION BY symbol ORDER BY ts)")
            #vdf.eval(name = f"LAG_{col}_vshort_ema", expr = f"LAG({col}_vshort_ema, 1, Null) OVER(PARTITION BY symbol ORDER BY ts)")
            vdf.eval(name = f"LAG_{col}_short_ema_D{lag}", expr = f"LAG({col}_short_ema, {lag}, Null) OVER(PARTITION BY symbol ORDER BY ts)")
            vdf.eval(name = f"LAG_{col}_long_ema_D{lag}", expr = f"LAG({col}_long_ema, {lag}, Null) OVER(PARTITION BY symbol ORDER BY ts)")
            #vdf.eval(name = f"LAG_{col}_vlong_ema_D{lag}", expr = f"LAG({col}_vlong_ema, {lag}, Null) OVER(PARTITION BY symbol ORDER BY ts)")
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

    ## MATERIALIZE
    vdf = vdf.select(["ts", "symbol"]+input_columns+[c for c in vdf.get_columns(input_columns) if c.startswith("\"LAG_")])
    vdf.dropna(columns=vdf.get_columns(input_columns))
    return materializeVdf(vdf)


def trainSymbolModels(target_symbol='all'):
    ## =========================================== ###
    output_models = {}
    max_iterations = 100
    correlation_threashold = (0.100, 0.999)

    logging.info(f"Training new Linear Regression models")
    training_vdf, model_columns = createTrainingData(target_symbol)

    logging.info("Analyzing columns correlations...")
    ## https://matplotlib.org/3.1.0/tutorials/colors/colormaps.html
    for out_col in output_columns:
        model_name = f"\"{db_schema}\".\"LR_{symbolToTable(target_symbol)}_{out_col}\""
        
        if out_col == "volume":
            correlation_threashold = (0.100, 0.999)

        # filtrar model_columns baseado no valor das correlations
        all_correlations = training_vdf.corr(columns = model_columns, focus=out_col, show=False).values
        best_correlations = [str(c).replace('"','') for i, c in enumerate(all_correlations.get('index')) if all_correlations.get(f'"{out_col}"')[i] >= correlation_threashold[0] and all_correlations.get(f'"{out_col}"')[i] <= correlation_threashold[1] ]
        _model_columns = [c for c in model_columns if c != out_col and c in best_correlations and (c.find(out_col)>0 or (out_col == "volume" and c.find("close")>0) or c.endswith("_all")) ]

        logging.info(f"{out_col}: {_model_columns}")
        
        if len(_model_columns) > 0:
            logging.info(f"Creating model: {model_name} [{len(_model_columns)} cols]")
            model = LinearRegression(model_name, max_iter=max_iterations, solver='Newton')
            try: model.drop()
            except: logging.info("Training new Model...")
            try: model.fit(training_vdf.current_relation(), _model_columns, out_col)
            except:
                logging.warn("Error using Newton solver, trying BFGS...")
                model = LinearRegression(model_name, max_iter=max_iterations, solver='BFGS')
                model.fit(training_vdf.current_relation(), _model_columns, out_col)

            logging.info(f"Model '{out_col}' Score: {model.score(method='r2')}")
            output_models[out_col] = model
        else:
            logging.warn(f"No column relevant for model: {model_name}!")
    return output_models
    
def createTrainingData(target_symbol):
    logging.info("Preparing the training data...")
    input_columns = ["close", "open", "high", "low", "volume"]

    vdf = vDataFrame(getRelation(input_table)).asfreq('ts', '1 day', {
        'volume' : 'ffill',
        'close' : 'ffill',
        'open' : 'ffill',
        'high' : 'ffill',
        'low' : 'ffill'
        }, by=['symbol']).sort({"ts": "desc"})

    if vdf.empty():
        logging.warn("No data points!")
        return vdf, []

    vdf_existing_cols = [str(c).replace('"','') for c in vdf.get_columns()]
    input_columns = [c for c in input_columns if c in vdf_existing_cols]

    # create all needed lag columns for weeks and months analysis
    print(vdf)
    input("pause")
    tmp = calculateColumns(vdf)

    # normalize only model columns
    model_columns = vdf.get_columns(["ts", "symbol"]+input_columns)
    model_columns = [str(c).replace('"','') for c in model_columns]
    model_columns = [c for c in model_columns if c.startswith("LAG_")]
    #vdf.normalize(columns = [c for c in model_columns if c.find("volume")>0 ], method = "minmax")
    vdf_final_columns = ["ts", "symbol"]+input_columns+model_columns

    # save vdf to database, as a table
    new_vdf_relation = dropTable(f"{input_table}__training")
    vdf.to_db(new_vdf_relation, usecols=vdf_final_columns, relation_type="table", inplace=True)
    logging.info(f"Dataframe saved as: {new_vdf_relation}")
    dropTable(tmp)

    return vdf, model_columns


def simulateSymbolData(target_symbol, output_models, days_to_simulate):
    logging.info(f"Simulating {target_symbol} prices for {days_to_simulate} days")

    if len(output_models) > 0:
        simul_vdf_table, next_day = createSimulationTable(target_symbol, days_to_simulate=days_to_simulate)
        for _i in range(days_to_simulate+1):
            logging.info(f"Simulating point {_i}...")
            
            # SIMULATE NEXT DATA POINT
            vdf = vDataFrame(simul_vdf_table)

            # create all needed lag columns for weeks and months analysis
            vdf.filter(conditions = [f"ts >= ADD_MONTHS('{next_day}', -24)::TIMESTAMP"])
            tmp = calculateColumns(vdf)
            
            # select only ts, symbol and model input/output columns
            vdf_existing_cols = [str(c).replace('"','') for c in vdf.get_columns()]
            _input_columns = [c for c in input_columns if c in vdf_existing_cols]
            model_columns = vdf.get_columns(["ts", "symbol"]+_input_columns)
            model_columns = [str(c).replace('"','') for c in model_columns]
            model_columns = [c for c in model_columns if c.startswith("LAG_")]

            vdf = vdf.select(["ts", "symbol"]+model_columns, False)
            vdf.filter(conditions = [f"ts >= ADD_MONTHS('{next_day}', -2)::TIMESTAMP"])
            vdf.sort({"ts": "desc"})

            outpt_columns = [c for c in output_columns if output_models.get(c, None) != None]
            for out_col in outpt_columns:
                output_models[out_col].predict(vdf, name = f"pred_{out_col}")

            # post simulation processing
            vdf.eval(name = "volume", expr = "ABS(pred_volume)::INTEGER")
            #else: vdf.eval(name = "volume", expr = "ABS(volume_D1)::INTEGER")

            vdf.eval(name = "close", expr = "ABS(pred_close)")
            #else: vdf.eval(name = "close", expr = "ROUND(ABS(close_D1), 2)")

            vdf.eval(name = "open", expr = "( LAG(close, 1, 1) OVER(PARTITION BY symbol ORDER BY ts) * 0.7)  + ( ABS(pred_open) * 0.3 )")
            #else: vdf.eval(name = "open", expr = "ROUND(ABS(open_D1), 2)")

            vdf.eval(name = "low",  expr = "apply_min(ARRAY[ open, close, ABS(pred_high), ABS(pred_low) ])")
            vdf.eval(name = "high", expr = "apply_max(ARRAY[ open, close, ABS(pred_high), ABS(pred_low) ])")
            
            '''
            if "high" in outpt_columns: 
                if "low" in outpt_columns: 
                    vdf.eval(name = "a_low",  expr = "apply_min(ARRAY[ open, close, ABS(pred_high), ABS(pred_low) ])")
                    vdf.eval(name = "a_high", expr = "apply_max(ARRAY[ open, close, ABS(pred_high), ABS(pred_low) ])")
                else: 
                    vdf.eval(name = "a_low",  expr = "apply_min(ARRAY[ open, close, ABS(pred_high), ABS(low_D1) ])")
                    vdf.eval(name = "a_high", expr = "apply_max(ARRAY[ open, close, ABS(pred_high), ABS(low_D1) ])")
            else:
                if "low" in outpt_columns: 
                    vdf.eval(name = "a_low",  expr = "apply_min(ARRAY[ open, close, ABS(high_D1), ABS(pred_low) ])")
                    vdf.eval(name = "a_high", expr = "apply_max(ARRAY[ open, close, ABS(high_D1), ABS(pred_low) ])")
                else: 
                    vdf.eval(name = "a_low",  expr = "apply_min(ARRAY[ open, close, ABS(high_D1), ABS(low_D1) ])")
                    vdf.eval(name = "a_high", expr = "apply_max(ARRAY[ open, close, ABS(high_D1), ABS(low_D1) ])")
            vdf.eval(name = "high", expr = "EXPONENTIAL_MOVING_AVERAGE(a_high, 0.2) OVER (PARTITION BY symbol ORDER BY ts) * 1.01")
            vdf.eval(name = "low", expr = "EXPONENTIAL_MOVING_AVERAGE(a_low, 0.2) OVER (PARTITION BY symbol ORDER BY ts) * 0.99")
            '''
            



            # get only this single simulated point
            vdf.filter(conditions = [f"\"{c}\" is not Null" for c in outpt_columns]+[f"ts >= '{next_day}'::TIMESTAMP"])

            # save vdf to database, as a table
            simul_res_table = getRelation(f"{input_table}_{symbolToTable(target_symbol)}_prediction")
            dropTable(simul_res_table)
            vdf.to_db(simul_res_table, usecols=["ts", "symbol"]+outpt_columns, relation_type="table", inplace=True)
            dropTable(tmp)
            logging.info(f"Merging {simul_res_table} into {simul_vdf_table}")
            mergeSimulationData(simul_vdf_table, simul_res_table)

        # FINSIHED
        return next_day
        
    else:
        logging.error("no models found!")
        return None


def createSimulationTable(target_symbol, inpt_table=input_table, days_to_simulate=5):
    logging.info(f"Creating {target_symbol} simulation data...")
    inpt_table = getRelation(inpt_table)

    # save vdf to database, as a table, on the first iteration
    simul_vdf_table = getRelation(f"{input_table}_{symbolToTable(target_symbol)}_simulation")
    input_colums_query = ','.join([f" \"{c}\"" for c in input_columns])
    null_colums_query = ','.join([f" Null as \"{c}\"" for c in input_columns])
    # generate next data points
    from_day = getDbLastTimestamp(target_symbol, inpt_table)
    next_day = next_business_day(from_day.date())
    to_day = next_day + datetime.timedelta(days=days_to_simulate-1)
    logging.info(f"from_day: {from_day}, next_business_day: {next_day}, to_day: {to_day}, input_table: {inpt_table}")

    ## MATERIALIZE
    dropTable(simul_vdf_table)
    input_table_query = f"( (SELECT ts, symbol,{input_colums_query} FROM {inpt_table} WHERE symbol = '{target_symbol}' and close is not null) UNION ALL (SELECT '{to_day}'::TIMESTAMP as ts, '{target_symbol}' as symbol,{null_colums_query}) ) \"input_table\""
    input_colums_query_ts = ','.join([f" CASE WHEN slice_time <= '{from_day}' THEN ROUND(TS_FIRST_VALUE(\"{c}\", 'linear'), 2) WHEN slice_time < '{next_day}' THEN ROUND(TS_LAST_VALUE(\"{c}\"), 2) ELSE Null END as \"{c}\"" for c in input_columns])
    query_select_tmp = f"SELECT slice_time as ts, symbol,{input_colums_query_ts} FROM {input_table_query} TIMESERIES slice_time AS '1 day' OVER(PARTITION by symbol ORDER BY ts)"
    query_create_tmp = f"CREATE TABLE {simul_vdf_table} as {query_select_tmp}"
    logging.info(query_create_tmp)
    with vertica_connection.cursor() as vert_cur:
        vert_cur.execute(query_create_tmp)

    # TODO create projections in simul_vdf_table
    logging.info(f"{target_symbol} Simulation data created!")

    return simul_vdf_table, next_day





## PROCESS ALL TARGET SYMBOLS
runProcess()

