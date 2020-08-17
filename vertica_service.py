# %%
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
from verticapy.learn.linear_model import ElasticNet
from verticapy.learn.decomposition import PCA
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
key_columns = ["ts", "symbol"]
input_columns = ["open", "close", "high", "low", "volume"]
output_columns = ["open", "close", "high", "low", "volume"]

pca_models = {}
enet_models = {}

new_auto_connection(conn_info, method = "vertica_python", name = "VerticaDSN")
change_auto_connection("VerticaDSN")

vertica_connection = vertica_python.connect(**conn_info)
#vert_cur = vertica_connection.cursor()

executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
logging.basicConfig(format='%(asctime)s (%(threadName)s) %(levelname)s - %(message)s', level=logging.INFO, handlers=[logging.FileHandler(logfile, 'w', 'utf-8')])

# https://towardsdatascience.com/machine-learning-techniques-applied-to-stock-price-prediction-6c1994da8001
# https://link.springer.com/article/10.1186/s40854-019-0137-1
# https://iceecs2018.org/wp-content/uploads/2019/10/paper_113.pdf

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
        trainModels()
        simulateNextDay()
            
    else:
        ### ONLY TRAIN MODELS ###
        if train_models:
            trainModels()
        

    logging.info("========= PROCESS FINISHED ========")
    
  

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
    logging.debug(f"Next workday: {next_day}")
    return next_day

def next_holiday_day(ref_date=datetime.date.today()):
    next_day = ref_date + ONE_DAY
    while next_day.weekday() not in holidays.WEEKEND and next_day not in HOLIDAYS_BR:
        next_day += ONE_DAY
    logging.debug(f"Next holiday: {next_day}")
    return next_day

def getDbLastTimestamp(symbol='%', table_name="daily_prices", column_name="ts"):
    with vertica_connection.cursor() as vert_cur: 
        return vert_cur.execute(f"SELECT COALESCE(MAX(\"{column_name}\"), '1990-01-01') as \"{column_name}\" FROM {getRelation(table_name)} WHERE symbol like '{symbol}' and close is not null;").fetchone()[0]

def mergeSimulationData(simulation_table, simulation_result_table):
    with vertica_connection.cursor() as vert_cur:
        return vert_cur.execute(f"MERGE INTO {getRelation(simulation_table)} sim USING {getRelation(simulation_result_table)} res ON sim.ts = res.ts and sim.symbol = res.symbol WHEN MATCHED THEN UPDATE SET open = res.open, close = res.close, high = res.high, low = res.low, volume = res.volume WHEN NOT MATCHED THEN INSERT (ts, symbol, open, close, high, low, volume) VALUES (res.ts, res.symbol, res.open, res.close, res.high, res.low, res.volume);")

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
    model_columns = [c for c in model_columns if c.startswith("LAG") or c in key_columns+input_columns ]
    return model_columns

def materializeVdf(vdf, tmp='None', target_symbol='', usecols=[]):
    prev_tmp = tmp
    tmp = dropTable(getRelation(generateTempName(target_symbol)))
    logging.info(f"Materializing temporary table {tmp}")
    vdf.to_db(tmp, usecols=usecols, relation_type="temporary", inplace=True)
    dropTable(prev_tmp)
    return tmp

def generateFeatures(vdf, n=20, m=2.0, lag=1):
    # n : Number of days in smoothing period (typically 20)
    # m : Number of standard deviations (typically 2)
    logging.info(f"Generating features [n: {n}, m: {m}, lag: {lag}]")
    
    # https://www.investopedia.com/terms/m/macd.asp
    # https://www.investopedia.com/terms/m/movingaverage.asp
    # https://www.mssqltips.com/sqlservertip/5441/using-tsql-to-detect-golden-crosses-and-death-crosses/
    
    # DAY AVERAGE
    logging.info(f"Generating feature 'Day Average'")
    vdf.eval(name = f"dayavg_close", expr = f"apply_avg(ARRAY[open,close,high,low])")
    vdf.eval(name = f"LAG_dayavg_close", expr = f"LAG(dayavg_close, {lag}) OVER(PARTITION BY symbol ORDER BY ts)")
    vdf.save()

    # DATE PARTS
    logging.info(f"Generating features 'Date Parts'")
    vdf.eval(name = "LAG_date_dow_all", expr = f"LAG(DATE_PART('ISODOW', ts), {lag}) OVER(PARTITION BY symbol ORDER BY ts)")
    vdf.eval(name = "LAG_date_woy_all", expr = f"LAG(DATE_PART('ISOWEEK', ts), {lag}) OVER(PARTITION BY symbol ORDER BY ts)")
    vdf.eval(name = "LAG_date_qtr_all", expr = f"LAG(DATE_PART('QUARTER', ts), {lag}) OVER(PARTITION BY symbol ORDER BY ts)")
    vdf.save()

    # https://wiki.timetotrade.com/Candlestick_Tail_Size
    logging.info(f"Generating features 'Candlestick'")
    vdf.eval(name = "candle_head", expr = "high - apply_max(ARRAY[open, close])")
    vdf.eval(name = "candle_body", expr = "apply_max(ARRAY[open, close]) - apply_min(ARRAY[open, close])")
    vdf.eval(name = "candle_tail", expr = "apply_min(ARRAY[open, close]) - low")
    vdf.eval(name = "LAG_chead_all", expr = f"LAG(candle_head, {lag}) OVER(PARTITION BY symbol ORDER BY ts)")
    vdf.eval(name = "LAG_cbody_all", expr = f"LAG(candle_body, {lag}) OVER(PARTITION BY symbol ORDER BY ts)")
    vdf.eval(name = "LAG_ctail_all", expr = f"LAG(candle_tail, {lag}) OVER(PARTITION BY symbol ORDER BY ts)")
    vdf.save()

    # https://www.fmlabs.com/reference/default.htm?url=RSI.htm
    logging.info(f"Generating feature 'RSI'")
    vdf.eval(name = "_clse_D1_0", expr = f"LAG(close, {lag}, 0) OVER(PARTITION BY symbol ORDER BY ts)")
    vdf.eval(name = "_up", expr = "CASE WHEN close > _clse_D1_0 THEN close - _clse_D1_0 ELSE 1 END")
    vdf.eval(name = "_dn", expr = "CASE WHEN close > _clse_D1_0 THEN 0 ELSE _clse_D1_0 - close END")
    vdf.eval(name = "_upavg", expr = f"AVG(_up) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '{n-1} days' PRECEDING AND CURRENT ROW)")
    vdf.eval(name = "_dnavg", expr = f"AVG(_dn) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '{n-1} days' PRECEDING AND CURRENT ROW)")
    vdf.eval(name = "LAG_RSI_all", expr = "ROUND(100 * ( _upavg / ( _upavg + _dnavg )), 2)")
    vdf.save()
 
    # LAG D-N
    max_lags = 5
    logging.info(f"Generating features 'Lags D-N ({lag} - {lag+max_lags})'")
    for col in input_columns:    
        for i in range(1, max_lags):
            vdf.eval(name = f"LAG_D{i}_{col}", expr = f"LAG({col}, {lag+i}) OVER(PARTITION BY symbol ORDER BY ts)")
    vdf.save()

    # https://www.fmlabs.com/reference/default.htm?url=SimpleMA.htm
    logging.info(f"Generating features 'SMA (N, 1W, 3M)'")
    for col in input_columns:
        vdf.eval(name = f"{col}_sma_N", expr = f"AVG({col}) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '{n} days' PRECEDING AND CURRENT ROW)")
        #vdf.eval(name = f"{col}_sma_3D", expr = f"AVG({col}) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '3 days' PRECEDING AND CURRENT ROW)")
        vdf.eval(name = f"{col}_sma_1W", expr = f"AVG({col}) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '1 week' PRECEDING AND CURRENT ROW)")
        #vdf.eval(name = f"{col}_sma_1M", expr = f"AVG({col}) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '1 month' PRECEDING AND CURRENT ROW)") 
        vdf.eval(name = f"{col}_sma_3M", expr = f"AVG({col}) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '3 month' PRECEDING AND CURRENT ROW)") 
        #vdf.eval(name = f"{col}_sma_6M", expr = f"AVG({col}) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '6 month' PRECEDING AND CURRENT ROW)") 
        #vdf.eval(name = f"{col}_sma_9M", expr = f"AVG({col}) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '9 month' PRECEDING AND CURRENT ROW)")
        #vdf.eval(name = f"{col}_sma_1Y", expr = f"AVG({col}) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '1 year' PRECEDING AND CURRENT ROW)")
        vdf.eval(name = f"LAG_{col}_sma_N_D{lag}", expr = f"LAG({col}_sma_N, {lag}) OVER(PARTITION BY symbol ORDER BY ts)")
        #vdf.eval(name = f"LAG_{col}_sma_3D_D{lag}", expr = f"LAG({col}_sma_3D, {lag}) OVER(PARTITION BY symbol ORDER BY ts)")
        vdf.eval(name = f"LAG_{col}_sma_1W_D{lag}", expr = f"LAG({col}_sma_1W, {lag}) OVER(PARTITION BY symbol ORDER BY ts)")
        #vdf.eval(name = f"LAG_{col}_sma_1M_D{lag}", expr = f"LAG({col}_sma_1M, {lag}) OVER(PARTITION BY symbol ORDER BY ts)")
        vdf.eval(name = f"LAG_{col}_sma_3M_D{lag}", expr = f"LAG({col}_sma_3M, {lag}) OVER(PARTITION BY symbol ORDER BY ts)")
        #vdf.eval(name = f"LAG_{col}_sma_6M_D{lag}", expr = f"LAG({col}_sma_6M, {lag}) OVER(PARTITION BY symbol ORDER BY ts)")
        #vdf.eval(name = f"LAG_{col}_sma_9M_D{lag}", expr = f"LAG({col}_sma_9M, {lag}) OVER(PARTITION BY symbol ORDER BY ts)")
        #vdf.eval(name = f"LAG_{col}_sma_1Y_D{lag}", expr = f"LAG({col}_sma_1Y, {lag}) OVER(PARTITION BY symbol ORDER BY ts)")
    vdf.save()
        
    # https://www.fmlabs.com/reference/default.htm?url=ExpMA.htm
    logging.info(f"Generating features 'ExpMA (short, long)'")
    for col in input_columns:
        #vdf.eval(name = f"{col}_vshort_ema", expr = f"EXPONENTIAL_MOVING_AVERAGE({col}, 0.2) OVER (PARTITION BY symbol ORDER BY ts)")
        vdf.eval(name = f"{col}_short_ema", expr = f"EXPONENTIAL_MOVING_AVERAGE({col}, 0.15) OVER (PARTITION BY symbol ORDER BY ts)")
        vdf.eval(name = f"{col}_long_ema", expr = f"EXPONENTIAL_MOVING_AVERAGE({col}, 0.075) OVER (PARTITION BY symbol ORDER BY ts)")
        #vdf.eval(name = f"{col}_vlong_ema", expr = f"EXPONENTIAL_MOVING_AVERAGE({col}, 0.0375) OVER (PARTITION BY symbol ORDER BY ts)")
        #vdf.eval(name = f"LAG_{col}_vshort_ema", expr = f"LAG({col}_vshort_ema, 1) OVER(PARTITION BY symbol ORDER BY ts)")
        vdf.eval(name = f"LAG_{col}_short_ema_D{lag}", expr = f"LAG({col}_short_ema, {lag}) OVER(PARTITION BY symbol ORDER BY ts)")
        vdf.eval(name = f"LAG_{col}_long_ema_D{lag}", expr = f"LAG({col}_long_ema, {lag}) OVER(PARTITION BY symbol ORDER BY ts)")
        #vdf.eval(name = f"LAG_{col}_vlong_ema_D{lag}", expr = f"LAG({col}_vlong_ema, {lag}) OVER(PARTITION BY symbol ORDER BY ts)")
    vdf.save()

    # https://www.fmlabs.com/reference/default.htm?url=MACD.ht
    '''
    logging.info(f"Generating features 'MACD (close, open, high, low, volume)'")
    for col in input_columns:
        vdf.eval(name = f"MACD", expr = f"{col}_short_ema - {col}_long_ema")
        vdf.eval(name = f"LAG_{col}_MACD", expr = f"LAG({col}_MACD, 1) OVER(PARTITION BY symbol ORDER BY ts)")
    vdf.save()
    '''
        
    

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

    vdf.eval(name = "LAG_STOCH_1M_close", expr = "LAG(STOCH_1M, 1, 0) OVER(PARTITION BY symbol ORDER BY ts)")
    vdf.eval(name = "LAG_STOCH_3M_close", expr = "LAG(STOCH_3M , 1, 0) OVER(PARTITION BY symbol ORDER BY ts)")
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
    vdf = vdf.select(key_columns+input_columns+[c for c in vdf.get_columns(input_columns) if c.startswith("\"LAG_")])
    vdf.dropna(columns=vdf.get_columns(input_columns))
    return materializeVdf(vdf)

def runFeatureDecomposition(vdf, model_columns, out_col):
    pca_model_name = getRelation(f"PCA_{out_col}__{input_table}")

    if pca_models.get(pca_model_name, None) == None:
        logging.info(f"Creating new PCA Model: {pca_model_name}")
        pca_models[pca_model_name] = PCA(pca_model_name)
        try: pca_models[pca_model_name].drop()
        except: logging.debug(f"Creating PCA Model in the database: {pca_model_name}")
        try: pca_models[pca_model_name].fit(vdf.current_relation(), model_columns+['idx'])    
        except: logging.error(f"Error Training PCA model {pca_model_name}")

    if pca_models.get(pca_model_name, None) != None:
        logging.info(f"Using PCA model: '{pca_model_name}' on '{out_col}'")
        pca_vdf = pca_models[pca_model_name].to_vdf(n_components=4, key_columns=['idx'])
        vdf = vdf.join(
            input_relation=pca_vdf,
            expr1=key_columns+input_columns,
            expr2=pca_vdf.get_columns(['idx']))
        vdf.sort({"symbol":"desc", "ts": "desc"})
        #print(pca_model.explained_variance)
        print(vdf)
        pca_training_table = dropTable(f"PCA_{out_col}_training__{input_table}")
        vdf.to_db(pca_training_table, relation_type="table", inplace=False)
        model_columns = [str(c).replace('"','') for c in vdf.get_columns(key_columns+input_columns)]
        return vDataframe(pca_training_table), model_columns

    return vdf, model_columns

def trainModels(inpt_table=input_table):
    ## =========================================== ###
    inpt_table = getRelation(inpt_table)
    max_iterations = 10000

    logging.info(f"Training new models ...")
    training_vdf, model_columns = createTrainingDataFrame()
    
    logging.info(f"Training features: {model_columns}")
    ## https://matplotlib.org/3.1.0/tutorials/colors/colormaps.html
    for out_col in output_columns:
        logging.info(f"Running '{out_col}' Feature Decomposition")
        _model_columns =  [c for c in model_columns if c != out_col and (c.find(out_col)>0 or c.endswith("_all")) ]
        _training_vdf, _model_columns = runFeatureDecomposition(training_vdf, _model_columns, out_col)

        enet_model_name = getRelation(f"ENET__{out_col}")
        logging.info(f"Creating model: {enet_model_name},  iter: {max_iterations}, n_cols: {len(_model_columns)}, cols: {_model_columns}")
        model = ElasticNet(enet_model_name, max_iter=max_iterations)#, solver='BFGS')
        try: model.drop()
        except: logging.info("Creating new ENET Model...")
        #try: model.fit(training_vdf.current_relation(), model_columns, out_col)
        #except: logging.error(f"Error Training ENET model {enet_model_name}")
        model.fit(_training_vdf.current_relation(), _model_columns, out_col)

        logging.info(f"Model '{out_col}' Score: {model.score(method='r2')}")
        enet_models[out_col] = model

    return training_vdf, model_columns
    
def createTrainingDataFrame():
    logging.info("Preparing the training data...")
    input_columns = ["close", "open", "high", "low", "volume"]

    vdf = vDataFrame(getRelation(input_table)).asfreq('ts', '1 day', {
        'volume' : 'ffill',
        'close' : 'ffill',
        'open' : 'ffill',
        'high' : 'ffill',
        'low' : 'ffill'
        }, by=['symbol'])

    if vdf.empty():
        logging.warning("No data points!")
        return vdf, []

    vdf_existing_cols = [str(c).replace('"','') for c in vdf.get_columns()]
    input_columns = [c for c in input_columns if c in vdf_existing_cols]

    # create all needed lag columns for weeks and months analysis
    print(vdf)
    tmp = generateFeatures(vdf)
    
    # normalize only model columns
    model_columns = vdf.get_columns(key_columns+input_columns)
    model_columns = [str(c).replace('"','') for c in model_columns]
    model_columns = [c for c in model_columns if c.startswith("LAG_")]
    vdf.dropna(model_columns)
    #vdf.normalize(columns = [c for c in model_columns if c.find("volume")>0 ], method = "minmax")
    #vdf.normalize(columns = model_columns, method = "minmax")
    vdf_final_columns = key_columns+input_columns+model_columns

    # create a index
    vdf.sort(key_columns).eval("idx", "ROW_NUMBER() OVER ()")

    # save vdf to database, as a table
    new_vdf_relation = dropTable(f"{input_table}__training")
    vdf.to_db(new_vdf_relation, usecols=["idx"]+vdf_final_columns, relation_type="table", inplace=True)
    logging.info(f"Dataframe saved as: {new_vdf_relation}")
    dropTable(tmp)

    return vdf, model_columns

def createSimulationDataFrame(simulation_name='default', inpt_table=input_table):
    logging.info(f"Creating simulation dataframe...")
    inpt_table = getRelation(inpt_table)
    simulation_table = dropTable(f"{input_table}__{simulation_name}_simulation")

    # generate next data points
    from_day = getDbLastTimestamp(table_name=inpt_table)
    next_day = next_business_day(from_day.date())
    #to_day = next_holiday_day(next_day) - datetime.timedelta(days=1)
    logging.info(f"Simulation: {from_day} -> {next_day} , input: {inpt_table}, output: {simulation_table}")
    new_data_points = vdf_from_relation(f"(select distinct '{next_day}'::TIMESTAMP ts, symbol, Null::NUMERIC open, Null::NUMERIC high, Null::NUMERIC low, Null::NUMERIC close, Null::NUMERIC volume from {inpt_table}) new_points")
    print(new_data_points)

    ## merge the historic data with the simulated points
    vdf = vDataFrame(getRelation(input_table)).filter(f"ts >= ADD_MONTHS('{from_day}', -24)::TIMESTAMP").append(new_data_points).asfreq('ts', '1 day', {
        'volume' : 'ffill',
        'close' : 'ffill',
        'open' : 'ffill',
        'high' : 'ffill',
        'low' : 'ffill'
        }, by=['symbol']).sort(key_columns).eval("idx", "ROW_NUMBER() OVER ()")

    ## MATERIALIZE
    vdf.to_db(simulation_table, relation_type="table", inplace=True)
    vdf.sort({"symbol":"desc", "ts": "desc"})
    
    # TODO create projections in simulation_table
    logging.info(f"Simulation Dataframe saved as: {simulation_table}")

    return vdf, simulation_table, next_day


def simulateNextDay(simulation_name='default'):
    logging.info(f"Running simulation '{simulation_name}'")

    if len(enet_models) <= 0:
        logging.error("no models found!")
        return None

    vdf, vdf_table, next_day = createSimulationDataFrame()

    # create all needed lag columns
    tmp = generateFeatures(vdf)
    vdf.filter(f"(ts = '{next_day}'::TIMESTAMP OR ts = '{next_day - datetime.timedelta(days=1)}'::TIMESTAMP)")

    # select 'ts', 'symbol' and model input&output columns
    vdf_cols = [str(c).replace('"','') for c in vdf.get_columns()]
    _input_columns = [c for c in input_columns if c in vdf_cols]
    model_columns = vdf.get_columns(key_columns+_input_columns)
    model_columns = [str(c).replace('"','') for c in model_columns]
    model_columns = [c for c in model_columns if c.startswith("LAG_")]
    
    outpt_columns = [c for c in output_columns if enet_models.get(c, None) != None]
    predicted_columns = {}
    for out_col in outpt_columns:
        logging.info(f"Running Feature Decomposition: {out_col}")
        _vdf, _model_columns = runFeatureDecomposition(vdf, model_columns, out_col)

        _vdf = _vdf.select(key_columns+_model_columns, False)
        pred_col =  f"pred_{out_col}"
        logging.debug(f"Predicting '{out_col}', cols: {_model_columns}")
        enet_models[out_col].predict(_vdf, name = pred_col)

        pred_value = _vdf.to_pandas().at[0,pred_col]
        logging.info(f"Predicted value of '{out_col}': {pred_value}")
        predicted_columns[out_col] = pred_value
        

    logging.info(f"Preparing '{simulation_name}' simulation results: {predicted_columns}")
    pred_sql_values = [f"{predicted_columns.get('pred_'+c, 'Null')} as \"{c}\"" for c in outpt_columns]
    logging.info(f"pred_sql_values: {pred_sql_values}")
    vdf = vdf_from_relation(f"(select {pred_sql_values}) predicted_results")
    print(vdf)
    # post simulation processing
    vdf.eval(name = "volume", expr = "ABS(pred_volume)::INTEGER")
    #else: vdf.eval(name = "volume", expr = "ABS(volume_D1)::INTEGER")
    vdf.eval(name = "close", expr = "ROUND(ABS(pred_close), 2)")
    #else: vdf.eval(name = "close", expr = "ROUND(ABS(close_D1), 2)")
    vdf.eval(name = "open", expr = "ROUND(( LAG(close, 1) OVER(PARTITION BY symbol ORDER BY ts) + ABS(pred_open) ) * 0.5, 2)")
    #else: vdf.eval(name = "open", expr = "ROUND(ABS(open_D1), 2)")
    vdf.eval(name = "low",  expr = "ROUND(apply_min(ARRAY[ open, close, ABS(pred_high), ABS(pred_low) ]), 2)")
    vdf.eval(name = "high", expr = "ROUND(apply_max(ARRAY[ open, close, ABS(pred_high), ABS(pred_low) ]), 2)")
 
    # save vdf to database, as a table
    simul_res_table = dropTable(f"{input_table}__{simulation_name}_prediction")
    vdf.to_db(simul_res_table, usecols=key_columns+outpt_columns, relation_type="table", inplace=True)
    dropTable(tmp)
    logging.info(f"Merging {simul_res_table} into {vdf_table}")
    mergeSimulationData(vdf_table, simul_res_table)
    
    return next_day




## PROCESS ALL TARGET SYMBOLS
runProcess()

