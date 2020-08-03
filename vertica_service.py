#%%
# pip3 install vertica-python verticapy holidays
import datetime
import holidays
import verticapy
from verticapy import *
from verticapy.toolbox import *
from verticapy.connections.connect import *
from verticapy.learn.linear_model import LinearRegression

ONE_DAY = datetime.timedelta(days=1)
HOLIDAYS_BR = holidays.BR()

def next_business_day():
    next_day = datetime.date.today() + ONE_DAY
    while next_day.weekday() in holidays.WEEKEND or next_day in HOLIDAYS_BR:
        next_day += ONE_DAY
    return next_day

new_auto_connection({'host': '127.0.0.1',
                    'port': 5433,
                    'user': 'dbadmin',
                    'password': 'stocks',
                    'database': 'stocks'}, 
                    method = "vertica_python", 
                    name = "VerticaDSN")
change_auto_connection("VerticaDSN")

## PROCESS ALL TARGET SYMBOLS
def createSymbolVdf(input_table, target_symbol, input_columns, increment_row=False):
    print("Preparing the input data...")

    #vdf = vDataFrame(input_table)
    input_colums_query = ','.join([f" TS_FIRST_VALUE(\"{c}\", 'linear') as \"{c}\"" for c in input_columns])
    vdf = vdf_from_relation(f"(SELECT slice_time as ts, symbol,{input_colums_query} FROM {input_table} TIMESERIES slice_time AS '1 day' OVER(PARTITION by symbol ORDER BY ts)) gapfilled")
    vdf.filter(conditions = [f"symbol = '{target_symbol}.SA'"])
    vdf.fillna(numeric_only = True)

    if increment_row:
        next_day = f"{next_business_day()} 00:00:00"
        initial_values = ','.join([f" \"{c}\"" for c in input_columns])
        nextday_query = f"(SELECT '{next_day}'::TIMESTAMP as ts, '{target_symbol}.SA' as symbol,{initial_values} FROM {input_table} WHERE symbol = '{target_symbol}.SA' ORDER BY {input_table}.ts DESC LIMIT 1) p"
        vdf = vdf.append(nextday_query)        
        print(vdf)

    vdf.sort({"ts": "desc"})
    vdf_existing_cols = [str(c).replace('"','') for c in vdf.get_columns()]
    input_columns = [c for c in input_columns if c in vdf_existing_cols]

    print(f"Generatig variables...")

    #  https://www.investopedia.com/terms/p/pricerateofchange.asp
    #vdf.eval(name = "roc", expr = f"((close - (LAG(close, 1, 0) OVER (PARTITION BY symbol ORDER BY ts)) )/(LAG(close, 1, 1) OVER (PARTITION BY symbol ORDER BY ts))) * 100")
    #if "roc3" not in input_columns:
    #vdf.eval(name = "roc3", expr = f"close - (LAG(close, 3, 0) OVER (PARTITION BY symbol ORDER BY ts))")
    #input_columns.append("roc3")

    for col in input_columns:
        print(f"Generatig derivatives from '{col}'...")
        # https://www.investopedia.com/terms/m/macd.asp
        # https://www.investopedia.com/terms/m/movingaverage.asp
        # https://www.mssqltips.com/sqlservertip/5441/using-tsql-to-detect-golden-crosses-and-death-crosses/
        vdf.eval(name = f"{col}_ema_01", expr = f"EXPONENTIAL_MOVING_AVERAGE({col}, 0.1) OVER (PARTITION BY symbol ORDER BY ts)")
        vdf.eval(name = f"{col}_ema_03", expr = f"EXPONENTIAL_MOVING_AVERAGE({col}, 0.3) OVER (PARTITION BY symbol ORDER BY ts)")
        vdf.eval(name = f"{col}_ema_05", expr = f"EXPONENTIAL_MOVING_AVERAGE({col}, 0.5) OVER (PARTITION BY symbol ORDER BY ts)")
        
        #vdf.eval(name = f"{col}_sma_3D", expr = f"AVG({col}) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '3 days' PRECEDING AND CURRENT ROW)")
        vdf.eval(name = f"{col}_sma_1W", expr = f"AVG({col}) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '1 week' PRECEDING AND CURRENT ROW)")
        vdf.eval(name = f"{col}_sma_2W", expr = f"AVG({col}) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '2 week' PRECEDING AND CURRENT ROW)")
        vdf.eval(name = f"{col}_sma_3W", expr = f"AVG({col}) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '3 week' PRECEDING AND CURRENT ROW)")
        vdf.eval(name = f"{col}_sma_1M", expr = f"AVG({col}) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '1 month' PRECEDING AND CURRENT ROW)")
        vdf.eval(name = f"{col}_sma_2M", expr = f"AVG({col}) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '2 month' PRECEDING AND CURRENT ROW)")
        vdf.eval(name = f"{col}_sma_3M", expr = f"AVG({col}) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '3 month' PRECEDING AND CURRENT ROW)")
        vdf.eval(name = f"{col}_sma_6M", expr = f"AVG({col}) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '6 month' PRECEDING AND CURRENT ROW)")
        vdf.eval(name = f"{col}_sma_1Y", expr = f"AVG({col}) OVER(PARTITION BY symbol ORDER BY ts RANGE BETWEEN INTERVAL '1 year' PRECEDING AND CURRENT ROW)")
        ## TODO comparar com o mesmo mes, e mesma semana dos anos anteriores
    
  
    # create all needed lag columns for weeks and months analysis
    # save vdf to database, as a table
    print("Saving dataframe in datatabase...")
    model_columns = vdf.get_columns(["ts", "symbol"]+input_columns)
    model_columns = [str(c).replace('"','') for c in model_columns]
    vdf.normalize(columns = model_columns, method = "robust_zscore")

    new_vdf_relation = f"{input_table}_{target_symbol}"
    drop_table(new_vdf_relation, print_info=False, raise_error=False)
    vdf.to_db(new_vdf_relation, relation_type="table", inplace=True)
    print(f"Dataframe saved as: {new_vdf_relation}")

    return vdf, model_columns



## =========================================== ###
input_table = "public.daily_prices"
target_symbol = "CVCB3"
input_columns = ["open", "close", "high", "low", "volume"]
output_columns = ["open", "close", "high", "low", "volume"]
output_models = {}

print("==================================================")
vdf, model_columns = createSymbolVdf(input_table, target_symbol, input_columns, True)

print("==================================================")
print("Analyzing variable correlations...")
correlation_threashold = 0.3

## https://matplotlib.org/3.1.0/tutorials/colors/colormaps.html
for out_col in output_columns:
    model_name = f"public.LR_{target_symbol}_{out_col}"
    
    # filtrar model_columns baseado no valor das correlations
    #vdf.corr(columns = model_columns, focus=out_col, cmap="RdYlGn")
    all_correlations = vdf.corr(columns = model_columns, focus=out_col, show=False).values
    best_correlations = [str(c).replace('"','') for i, c in enumerate(all_correlations.get('index')) if all_correlations.get(f'"{out_col}"')[i] >= correlation_threashold and all_correlations.get(f'"{out_col}"')[i] < 1.0 ]
    _model_columns = [c for c in model_columns if c != out_col and c in best_correlations]

    print(f"Creating model: {model_name} [{len(_model_columns)} cols]")
    model = LinearRegression(model_name)
    try: model.drop()
    except: print("Training new Model...")
    model.fit(vdf.current_relation(), _model_columns, out_col)
    print(f"Model '{out_col}' Score: {model.score(method='r2')}")
    output_models[out_col] = model

print("==================================================")
print("Predicting data...")

#ts,symbol,"open",high,low,"close",volume
#input_vdf = vDataFrame(input_table)

#_, _ = createSymbolVdf(input_table, target_symbol, input_columns, input_vdf)
#vdf.filter(conditions = [f"ts = '{next_day}'::TIMESTAMP"])
print(vdf)
for out_col in output_columns:
    output_models[out_col].predict(vdf, name = f"predicted_{out_col}")

print(vdf)
#vdf.hist(["close", "pred_close"], h = (2, 2))


'''
model = LinearRegression(f"public.LR_daily_prices_{target_symbol}")

model.fit(f"public.daily_prices_{target_symbol}", 
    [
        "open_D-1", "high_D-1", "low_D-1", "close_D-1", 
        "open_D-2", "high_D-2", "low_D-2", "close_D-2",          
        "open_D-3", "high_D-3", "low_D-3", "close_D-3"
    ], "close")


print(model)

model.score(method = "r2")
print(model.deploySQL())

model.regression_report()

model.predict(vdf, name = "pred_close")


vdf.hist(["close", "pred_close"], h = (2, 2))

#model.plot()

model.drop()
'''

# %%

