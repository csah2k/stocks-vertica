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
output_columns = [ "close" ] #, "open",  "high", "low", "volume"]

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

# file:///C:/Users/csah2k/Documents/Work/STOCKS/verticapy.com/examples/movies/index.html

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
    logging.info(f"Generating features [N: {n}, M: {m}, LAG: {lag}]")
    
    # https://www.investopedia.com/terms/m/macd.asp
    # https://www.investopedia.com/terms/m/movingaverage.asp
    # https://www.mssqltips.com/sqlservertip/5441/using-tsql-to-detect-golden-crosses-and-death-crosses/

    numeric_feats = [] # numeric features to NORMALIZE
    category_feats = [] # categorical features to ONE-HOT ENCODE
    model_feats = {}
    for col in input_columns:
        model_feats[col] = []
    
    # DAY AVERAGE
    logging.debug("Generating feature 'Typical price'")
    vdf.eval("TP", "apply_avg(ARRAY[close,high,low])")
    _lag = createLag(vdf, "TP", lag)
    for col in ['open','close','high','low']: model_feats[col].append(_lag)

    # DATE PARTS
    logging.debug(f"Generating features 'Date Parts'")
    for dtpart in ['ISODOW', 'QUARTER']:
        _col = f"DT_{dtpart}"
        vdf.eval(_col, f"DATE_PART('{dtpart}', ts)")
        _lag = createLag(vdf, _col, lag)
        for col in output_columns: model_feats[col].append(_lag)
        category_feats.append(_lag)

    # LAG D-N
    max_lags = 5
    logging.debug(f"Generating features 'Lags D-N (1 - {max_lags})'")
    for col in output_columns:    
        for i in range(1, max_lags):
            _lag = createLag(vdf, col, i)
            model_feats[col].append(_lag)
        if lag not in range(1, max_lags): ## assure main lag existence
            _lag = createLag(vdf, col, lag)
            model_feats[col].append(_lag)
        if n not in range(1, max_lags): ## assure lag N existence
            _lag = createLag(vdf, col, n)
            model_feats[col].append(_lag)

    # https://wiki.timetotrade.com/Candlestick_Tail_Size
    logging.debug(f"Generating features 'Candlestick'")
    vdf.eval("candle_head", "high - apply_max(ARRAY[open, close])")
    vdf.eval("candle_body", "apply_max(ARRAY[open, close]) - apply_min(ARRAY[open, close])")
    vdf.eval("candle_tail", "apply_min(ARRAY[open, close]) - low")
    for candle in ['candle_head', 'candle_body', 'candle_tail']:
        _lag = createLag(vdf, candle, lag)
        for col in output_columns: model_feats[col].append(_lag)
        numeric_feats.append(_lag)

    # https://www.fmlabs.com/reference/default.htm?url=RSI.htm
    logging.debug(f"Generating feature 'RSI'")
    vdf.eval("_up", "CASE WHEN close > LAG_D1_close THEN close - LAG_D1_close ELSE 0 END")
    vdf.eval("_dn", "CASE WHEN close < LAG_D1_close THEN LAG_D1_close - close ELSE 0 END")
    vdf.eval("_upavg", f"AVG(_up) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '{n} days' PRECEDING AND CURRENT ROW)")
    vdf.eval("_dnavg", f"AVG(_dn) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '{n} days' PRECEDING AND CURRENT ROW)")
    vdf.eval("RSI", "CASE WHEN ( _upavg + _dnavg ) <> 0 THEN ROUND(100 * ( _upavg / ( _upavg + _dnavg )), 2) ELSE 0 END")
    _lag = createLag(vdf, "RSI", lag)
    for col in output_columns: model_feats[col].append(_lag)
    numeric_feats.append(_lag) 

    # https://www.fmlabs.com/reference/default.htm?url=SimpleMA.htm
    logging.debug(f"Generating features 'SMA (N, 1W, 3M)'")
    for col in output_columns:
        for interv in [f'{n} days', '1 week', '3 month']:
            _interv = re.sub(r'\W', '', interv).upper()
            _col = f"SMA{_interv}_{col}"
            vdf.eval(_col, f"AVG({col}) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '{interv}' PRECEDING AND CURRENT ROW)")
            _lag = createLag(vdf, _col, lag)
            model_feats[col].append(_lag)
        
    # https://www.fmlabs.com/reference/default.htm?url=ExpMA.htm
    logging.debug(f"Generating features 'ExpMA (short, long)'")
    for col in output_columns:
        for ratio in ['0.075', '0.15']:
            _ratio = re.sub(r'\D', '', ratio)
            vdf.eval(f"EMA{_ratio}_{col}", f"EXPONENTIAL_MOVING_AVERAGE({col}, {ratio}) OVER (PARTITION BY symbol ORDER BY ts)")
            _lag = createLag(vdf, f"EMA{_ratio}_{col}", lag)
            model_feats[col].append(_lag)

    # https://www.fmlabs.com/reference/default.htm?url=MACD.ht
    # https://www.fidelity.com/learning-center/trading-investing/technical-analysis/technical-indicator-guide/apo
    logging.debug(f"Generating features 'MACD'")
    for col in output_columns:
        vdf.eval(f"MACD_{col}", f"EMA015_{col} - EMA0075_{col}")
        _lag = createLag(vdf, f"MACD_{col}", lag)
        model_feats[col].append(_lag)
        numeric_feats.append(_lag) 

    #  https://www.investopedia.com/terms/p/pricerateofchange.asp
    logging.debug(f"Generating feature 'ROC'")
    for col in output_columns:
        vdf.eval("ROC", f"({col} - (LAG({col}, {lag}, 0) OVER(PARTITION BY symbol ORDER BY ts)) ) / (LAG({col}, {lag}, 1) OVER(PARTITION BY symbol ORDER BY ts)) * 100")
        for ratio in ['0.075', '0.15']:
            _ratio = re.sub(r'\D', '', ratio)
            vdf.eval(f"ROC_EMA{_ratio}_{col}", f"EXPONENTIAL_MOVING_AVERAGE(ROC, {ratio}) OVER (PARTITION BY symbol ORDER BY ts)")
            _lag = createLag(vdf, f"ROC_EMA{_ratio}_{col}", lag)
            model_feats[col].append(_lag)
            numeric_feats.append(_lag)

    # https://www.fmlabs.com/reference/default.htm?url=Momentum.htm
    logging.debug(f"Generating feature 'Momentum'")
    for col in output_columns:
        _col = f"MOMENTUM_{col}"
        vdf.eval(_col, f"{col} - LAG_D{n}_{col}")
        _lag = createLag(vdf, _col, lag)
        model_feats[col].append(_lag)
        numeric_feats.append(_lag)

    # https://www.investopedia.com/terms/v/vwap.asp
    logging.debug(f"Generating feature 'VWAP'")
    vdf.eval("VWAP", "(volume * ((high + low + close) / 3)) / volume")
    _lag = createLag(vdf, "VWAP", lag)
    for col in output_columns: model_feats[col].append(_lag)
    numeric_feats.append(_lag)
  

    # https://www.fmlabs.com/reference/default.htm?url=StochasticOscillator.htm
    logging.debug(f"Generating feature 'STOCH'")
    vdf.eval("highest_high", f"MAX(high) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '{n} days' PRECEDING AND CURRENT ROW)")
    vdf.eval("lowest_low", f"MIN(low) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '{n} days' PRECEDING AND CURRENT ROW)")
    vdf.eval("stch_k", "100 * ((close - lowest_low ) / (highest_high - lowest_low))")
    for ratio in ['0.075', '0.15']:
        _ratio = re.sub(r'\D', '', ratio)
        vdf.eval(f"STOCH_EMA{_ratio}", f"EXPONENTIAL_MOVING_AVERAGE(stch_k, {ratio}) OVER (PARTITION BY symbol ORDER BY ts)")
        _lag = createLag(vdf, f"STOCH_EMA{_ratio}", lag)
        for col in output_columns: model_feats[col].append(_lag)
        numeric_feats.append(_lag)


    # https://www.fmlabs.com/reference/default.htm?url=OBV.htm
    logging.debug(f"Generating feature 'OBV'")
    vdf.eval("_bv", "CASE WHEN close > LAG_D1_close THEN ABS(volume) WHEN close < LAG_D1_close THEN ABS(volume) * -1 ELSE 0 END")
    vdf.eval("OBV", "LAG(_bv, 1) OVER(PARTITION BY symbol ORDER BY ts) + _bv")
    _lag = createLag(vdf, "OBV", lag)
    for col in output_columns: model_feats[col].append(_lag)
    numeric_feats.append(_lag)

    # https://www.fmlabs.com/reference/default.htm?url=DI.htm
    logging.debug(f"Generating feature 'DI'")
    vdf.eval("_delta_high", "LAG(high, 1) OVER(PARTITION BY symbol ORDER BY ts) - high")
    vdf.eval("_delta_low", "low - LAG(low, 1) OVER(PARTITION BY symbol ORDER BY ts)")
    vdf.eval("_plus_dm", "CASE WHEN ( _delta_high < 0 AND _delta_low < 0 ) OR _delta_high = _delta_low THEN 0 WHEN _delta_high > _delta_low THEN _delta_high ELSE 0 END")
    vdf.eval("_minus_dm", "CASE WHEN ( _delta_high < 0 AND _delta_low < 0 ) OR _delta_high = _delta_low THEN 0 WHEN _delta_high > _delta_low THEN 0 ELSE _delta_low END")
    vdf.eval("_plus_dm_sum", f"LAG(_plus_dm, 1) OVER(PARTITION BY symbol ORDER BY ts) - (LAG(_plus_dm, 1) OVER(PARTITION BY symbol ORDER BY ts) / {n}) + _plus_dm")
    vdf.eval("_minus_dm_sum", f"LAG(_minus_dm, 1) OVER(PARTITION BY symbol ORDER BY ts) - (LAG(_minus_dm, 1) OVER(PARTITION BY symbol ORDER BY ts) / {n}) + _minus_dm")
    vdf.eval("_tr",  "apply_max(ARRAY[ high, LAG_D1_close ]) - apply_min(ARRAY[ low,  LAG_D1_close ])")
    vdf.eval("_tr_sum", f"LAG(_tr, 1) OVER(PARTITION BY symbol ORDER BY ts) - (LAG(_tr, 1) OVER(PARTITION BY symbol ORDER BY ts) / {n}) + _tr")
    vdf.eval("pDI",  "CASE WHEN _tr_sum = 0 THEN 0 ELSE (100 * (_plus_dm_sum / _tr_sum)) END")
    vdf.eval("mDI",  "CASE WHEN _tr_sum = 0 THEN 0 ELSE (100 * (_minus_dm_sum / _tr_sum)) END")
    # https://www.fmlabs.com/reference/default.htm?url=DX.htm
    logging.debug(f"Generating feature 'DX'")
    vdf.eval("DX",  "CASE WHEN (pDI + mDI) = 0 THEN 0 ELSE ((pDI - mDI) / (pDI + mDI)) END")
    _lag = createLag(vdf, "DX", lag)
    for col in output_columns: model_feats[col].append(_lag)
    numeric_feats.append(_lag)
    # https://www.fmlabs.com/reference/default.htm?url=ADX.htm
    logging.debug(f"Generating feature 'ADX'")
    vdf.eval("ADX", f"AVG(DX) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '{n} days' PRECEDING AND CURRENT ROW)")
    _lag = createLag(vdf, "ADX", lag)
    for col in output_columns: model_feats[col].append(_lag)
    numeric_feats.append(_lag)
    # https://www.fmlabs.com/reference/default.htm?url=ADXR.htm
    logging.debug(f"Generating feature 'ADXR'")
    vdf.eval("ADXR", f"apply_avg(ARRAY[ ADX, LAG(ADX, {n}) OVER(PARTITION BY symbol ORDER BY ts) ] )")
    _lag = createLag(vdf, "ADXR", lag)
    for col in output_columns: model_feats[col].append(_lag)
    numeric_feats.append(_lag)


    # https://www.marketvolume.com/stocks/bop.asp
    logging.debug(f"Generating feature 'BOP'")
    vdf.eval("_bop", f"CASE WHEN (high - low) = 0 THEN 0 ELSE (close - open) / (high - low) END")
    vdf.eval("BOP", f"AVG(_bop) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '{n} days' PRECEDING AND CURRENT ROW)")
    _lag = createLag(vdf, "BOP", lag)
    for col in output_columns: model_feats[col].append(_lag)
    numeric_feats.append(_lag)

    # https://www.investopedia.com/articles/active-trading/031914/how-traders-can-utilize-cci-commodity-channel-index-trade-stock-trends.asp
    # https://www.fidelity.com/learning-center/trading-investing/technical-analysis/technical-indicator-guide/cci
    # CCI = (TP - SMA_of_TP) / (0.015 * Mean Deviation)
    logging.debug(f"Generating feature 'CCI'")
    vdf.eval("SMATP", f"AVG(TP) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '{n} days' PRECEDING AND CURRENT ROW)")
    vdf.eval("MEDEV", f"AVG(ABS(SMATP-TP)) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '{n} days' PRECEDING AND CURRENT ROW)")    
    vdf.eval("CCI", "CASE WHEN MEDEV <> 0 THEN (TP - SMATP) / (0.015 * MEDEV) ELSE 0 END")
    _lag = createLag(vdf, "CCI", lag)
    for col in output_columns: model_feats[col].append(_lag)
    numeric_feats.append(_lag)

    # https://www.fmlabs.com/reference/default.htm?url=CMO.htm
    logging.debug(f"Generating feature 'CMO'")
    vdf.eval("_ups", f"SUM(_up) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '{n} days' PRECEDING AND CURRENT ROW)")
    vdf.eval("_downs", f"SUM(_dn) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '{n} days' PRECEDING AND CURRENT ROW)")
    vdf.eval("CMO", "CASE WHEN (_ups + _downs) <> 0 THEN (100 * ((_ups - _downs)/(_ups + _downs)) ) ELSE 0 END")
    _lag = createLag(vdf, "CMO", lag)
    for col in output_columns: model_feats[col].append(_lag)
    numeric_feats.append(_lag)

    # https://www.fmlabs.com/reference/default.htm?url=MFI.htm
    logging.debug(f"Generating feature 'MFI'")
    vdf.eval("MFI", "CASE WHEN volume <> 0 THEN (high - low) / volume ELSE 0 END")
    _lag = createLag(vdf, "MFI", lag)
    for col in output_columns: model_feats[col].append(_lag)
    numeric_feats.append(_lag)
    
    # https://www.fmlabs.com/reference/default.htm?url=DMI.htm
    logging.debug(f"Generating feature 'DMI'")

    # https://www.fmlabs.com/reference/default.htm?url=WilliamsR.htm
    logging.debug(f"Generating feature 'Williams %R'")

    # https://www.fmlabs.com/reference/default.htm?url=ATR.htm
    logging.debug(f"Generating feature 'ATR'")

    # https://www.investopedia.com/terms/a/aroon.asp
    logging.debug(f"Generating feature 'AROON'")

    # https://www.fmlabs.com/reference/default.htm?url=ChaikinVolatility.htm
    # https://www.fmlabs.com/reference/default.htm?url=ChaikinMoneyFlow.htm
    # https://www.fmlabs.com/reference/default.htm?url=ChaikinOscillator.htm


    # Last step before exporting the data is to normalize the numerical columns and to get the dummies of the different categories.
    logging.info(f"Normalizing numeric fetures: {numeric_feats}")
    vdf.normalize(method = "minmax", columns = numeric_feats)
    logging.info(f"Creatings dummies for category fetures: {category_feats}")
    vdf.get_dummies(drop_first = True, columns = category_feats)

    #vdf.drop(category_feats)
    new_dummy_feats = {}
    for dummy in [c.replace('"', '') for c in vdf.get_columns(key_columns+input_columns+numeric_feats+category_feats)]:
        col = re.sub(r'_\d+$', '', dummy)
        if col in category_feats:
            new_dummy_feats[col] = new_dummy_feats.get(col,[]) + [dummy]

    #logging.info(f"new_dummy_feats: {new_dummy_feats}")
    vdf.drop(category_feats)
    for model in model_feats:
        for col in category_feats:
            if col in model_feats[model]:
                model_feats[model].remove(col)
                model_feats[model] += new_dummy_feats[col]

    ## flatten the list of lists and dedup
    all_models_feats = []
    for col in model_feats:
        all_models_feats += model_feats[col]
    all_models_feats = list(set(all_models_feats))

    ## MATERIALIZE
    vdf = vdf.select(key_columns+input_columns+all_models_feats) #[c for c in vdf.get_columns(input_columns) if c.startswith("\"LAG_")])
    vdf.dropna(columns=vdf.get_columns(input_columns))
    return materializeVdf(vdf), model_feats, all_models_feats

def createLag(vdf:vDataFrame, col:str, lag=1):
    _col = f'LAG_D{lag}_{col}'
    vdf.eval(_col, f"LAG({col}, {lag}) OVER(PARTITION BY symbol ORDER BY ts)")
    return _col

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
        pca_vdf = pca_models[pca_model_name].to_vdf(key_columns=['idx'])
        vdf = vdf.join(
            input_relation=pca_vdf,
            expr1=key_columns+input_columns+model_columns,
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
    max_iterations = 1000

    logging.info(f"Training new models ...")
    training_vdf, models_feats, all_models_feats = createTrainingDataFrame()
    
    logging.info(f"Training features: {len(all_models_feats)}")
    ## https://matplotlib.org/3.1.0/tutorials/colors/colormaps.html
    for out_col in output_columns:
        #logging.info(f"Running '{out_col}' Feature Decomposition")
        #_model_columns =  [c for c in model_columns if c != out_col and (c.find(out_col)>0 or c.endswith("_all")) ]
        #_training_vdf, _model_columns = runFeatureDecomposition(training_vdf, _model_columns, out_col)
        _model_columns = models_feats[out_col]
        _training_vdf = training_vdf.select(key_columns+input_columns+_model_columns).to_db(dropTable(f"ENET__{out_col}_training"), relation_type="table", inplace = True)

        enet_model_name = getRelation(f"ENET__{out_col}")
        logging.info(f"Creating model: {enet_model_name},  iter: {max_iterations}, n_cols: {len(_model_columns)}, cols: {_model_columns}")
        model = ElasticNet(enet_model_name, max_iter=max_iterations, penalty='None', solver='BFGS')
        try: model.drop()
        except: logging.info("Creating new ENET Model...")
        #try: model.fit(training_vdf.current_relation(), model_columns, out_col)
        #except: logging.error(f"Error Training ENET model {enet_model_name}")
        model.fit(_training_vdf.current_relation(), _model_columns, out_col)
        print(model.regression_report())
        logging.info(f"Model '{out_col}' Score: {model.score(method='r2')}")
        enet_models[out_col] = model

    return training_vdf, models_feats
    
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
        return vdf, {}, []

    vdf_existing_cols = [str(c).replace('"','') for c in vdf.get_columns()]
    input_columns = [c for c in input_columns if c in vdf_existing_cols]

    # create all needed lag columns for weeks and months analysis
    print(vdf)
    tmp, models_feats, all_models_feats = generateFeatures(vdf)

    vdf.dropna(all_models_feats)
    # normalize only model columns
    #model_columns = vdf.get_columns(key_columns+input_columns)
    #model_columns = [str(c).replace('"','') for c in model_columns]
    #model_columns = [c for c in model_columns if c.startswith("LAG_")]
    #vdf.dropna(model_columns)
    #vdf.normalize(columns = [c for c in model_columns if c.find("volume")>0 ], method = "minmax")
    #vdf.normalize(columns = model_columns, method = "minmax")
    vdf_final_columns = key_columns+input_columns+all_models_feats

    # create a index
    #vdf.sort(key_columns).eval("idx", "ROW_NUMBER() OVER ()")

    # save vdf to database, as a table
    new_vdf_relation = dropTable(f"{input_table}__training")
    #vdf.to_db(new_vdf_relation, usecols=["idx"]+vdf_final_columns, relation_type="table", inplace=True)
    vdf.to_db(new_vdf_relation, usecols=vdf_final_columns, relation_type="table", inplace=True)
    logging.info(f"Dataframe saved as: {new_vdf_relation}")
    dropTable(tmp)

    return vdf, models_feats, all_models_feats

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
        }, by=['symbol']).sort(key_columns)#.eval("idx", "ROW_NUMBER() OVER ()")

    ## MATERIALIZE
    vdf.to_db(simulation_table, relation_type="table", inplace=True)
    vdf.sort({"symbol":"desc", "ts": "desc"})
    
    # TODO create projections in simulation_table
    logging.info(f"Simulation Dataframe saved as: {simulation_table}")

    return vdf, simulation_table, next_day


def simulateNextDay(simulation_name='default'):
    logging.info(f"========= Running simulation '{simulation_name}' ========= ")

    if len(enet_models) <= 0:
        logging.error("no models found!")
        return None

    vdf, _, next_day = createSimulationDataFrame()

    # create all needed lag columns
    tmp, models_feats, _ = generateFeatures(vdf)
    vdf.filter(f"(ts = '{next_day}'::TIMESTAMP OR ts = '{next_day - datetime.timedelta(days=1)}'::TIMESTAMP)")
    predicted_cols = []
    for out_col in [c for c in output_columns if enet_models.get(c, None) != None]:
        #logging.info(f"Running Feature Decomposition: {out_col}")
        #_vdf, _model_columns = runFeatureDecomposition(vdf, model_columns, out_col)

        _model_columns = models_feats[out_col]
        pred_col =  f"pred_{out_col}"
        logging.debug(f"Predicting '{out_col}', cols: {_model_columns}")
        enet_models[out_col].predict(vdf, name = pred_col)
        predicted_cols.append(pred_col)

        #pred_value = vdf.to_pandas().at[0,pred_col]
        #logging.info(f"Predicted value of '{out_col}': {pred_value}")
    vdf = vdf.select(key_columns+input_columns+predicted_cols)
    print(vdf)


 
    # post simulation processing
    '''
    vdf.eval("volume", "ABS(pred_volume)::INTEGER")
    vdf.eval("close", "ROUND(ABS(pred_close), 2)")
    vdf.eval("open", "ROUND(( LAG(close, 1) OVER(PARTITION BY symbol ORDER BY ts) + ABS(pred_open) ) * 0.5, 2)")
    vdf.eval("low", "ROUND(apply_min(ARRAY[ open, close, ABS(pred_high), ABS(pred_low) ]), 2)")
    vdf.eval("high", "ROUND(apply_max(ARRAY[ open, close, ABS(pred_high), ABS(pred_low) ]), 2)")
 
    # save vdf to database, as a table
    simul_res_table = dropTable(f"{input_table}__{simulation_name}_prediction")
    vdf.to_db(simul_res_table, usecols=key_columns+outpt_columns, relation_type="table", inplace=True)
    dropTable(tmp)
    logging.info(f"Merging {simul_res_table} into {vdf_table}")
    mergeSimulationData(vdf_table, simul_res_table)
    '''
    
    return next_day




## PROCESS ALL TARGET SYMBOLS
runProcess()

