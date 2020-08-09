

CREATE PROJECTION daily_prices_DBD_1_rep_CMP01 /*+createtype(D)*/
(
 ts ENCODING COMMONDELTA_COMP, 
 symbol ENCODING RLE, 
 open ENCODING DELTARANGE_COMP, 
 high ENCODING COMMONDELTA_COMP, 
 low ENCODING COMMONDELTA_COMP, 
 close ENCODING DELTARANGE_COMP, 
 volume ENCODING AUTO
)
AS
 SELECT ts, 
        symbol, 
        open, 
        high, 
        low, 
        close, 
        volume
 FROM stocks.daily_prices 
 ORDER BY symbol,
          ts
UNSEGMENTED ALL NODES;

CREATE PROJECTION stock_symbols_DBD_2_rep_CMP01 /*+createtype(D)*/
(
 company ENCODING AUTO, 
 symbol ENCODING AUTO, 
 industry ENCODING AUTO, 
 headquarters ENCODING RLE
)
AS
 SELECT company, 
        symbol, 
        industry, 
        headquarters
 FROM stocks.stock_symbols 
 ORDER BY headquarters,
          symbol
UNSEGMENTED ALL NODES;

CREATE PROJECTION daily_prices_cvcbsa_training_DBD_3_rep_CMP01 /*+createtype(D)*/
(
 ts ENCODING GCDDELTA, 
 symbol ENCODING RLE, 
 close ENCODING AUTO, 
 open ENCODING AUTO, 
 high ENCODING AUTO, 
 low ENCODING AUTO, 
 volume ENCODING AUTO, 
 LAG_open_sma_1W ENCODING AUTO, 
 LAG_open_sma_1M ENCODING AUTO, 
 LAG_open_sma_3M ENCODING DELTARANGE_COMP, 
 LAG_open_sma_6M ENCODING DELTARANGE_COMP, 
 LAG_open_sma_1Y ENCODING DELTARANGE_COMP, 
 LAG_open_short_ema ENCODING DELTARANGE_COMP, 
 LAG_open_long_ema ENCODING DELTARANGE_COMP, 
 LAG_open_vlong_ema ENCODING DELTARANGE_COMP, 
 LAG_close_sma_1W ENCODING AUTO, 
 LAG_close_sma_1M ENCODING AUTO, 
 LAG_close_sma_3M ENCODING DELTARANGE_COMP, 
 LAG_close_sma_6M ENCODING DELTARANGE_COMP, 
 LAG_close_sma_1Y ENCODING DELTARANGE_COMP, 
 LAG_close_short_ema ENCODING DELTARANGE_COMP, 
 LAG_close_long_ema ENCODING DELTARANGE_COMP, 
 LAG_close_vlong_ema ENCODING DELTARANGE_COMP, 
 LAG_high_sma_1W ENCODING AUTO, 
 LAG_high_sma_1M ENCODING AUTO, 
 LAG_high_sma_3M ENCODING DELTARANGE_COMP, 
 LAG_high_sma_6M ENCODING DELTARANGE_COMP, 
 LAG_high_sma_1Y ENCODING DELTARANGE_COMP, 
 LAG_high_short_ema ENCODING DELTARANGE_COMP, 
 LAG_high_long_ema ENCODING DELTARANGE_COMP, 
 LAG_high_vlong_ema ENCODING DELTARANGE_COMP, 
 LAG_low_sma_1W ENCODING AUTO, 
 LAG_low_sma_1M ENCODING AUTO, 
 LAG_low_sma_3M ENCODING DELTARANGE_COMP, 
 LAG_low_sma_6M ENCODING DELTARANGE_COMP, 
 LAG_low_sma_1Y ENCODING DELTARANGE_COMP, 
 LAG_low_short_ema ENCODING DELTARANGE_COMP, 
 LAG_low_long_ema ENCODING DELTARANGE_COMP, 
 LAG_low_vlong_ema ENCODING DELTARANGE_COMP, 
 LAG_volume_sma_1W ENCODING AUTO, 
 LAG_volume_sma_1M ENCODING AUTO, 
 LAG_volume_sma_3M ENCODING AUTO, 
 LAG_volume_sma_6M ENCODING AUTO, 
 LAG_volume_sma_1Y ENCODING AUTO, 
 LAG_volume_short_ema ENCODING DELTARANGE_COMP, 
 LAG_volume_long_ema ENCODING DELTARANGE_COMP, 
 LAG_volume_vlong_ema ENCODING DELTARANGE_COMP
)
AS
 SELECT ts, 
        symbol, 
        close, 
        open, 
        high, 
        low, 
        volume, 
        LAG_open_sma_1W, 
        LAG_open_sma_1M, 
        LAG_open_sma_3M, 
        LAG_open_sma_6M, 
        LAG_open_sma_1Y, 
        LAG_open_short_ema, 
        LAG_open_long_ema, 
        LAG_open_vlong_ema, 
        LAG_close_sma_1W, 
        LAG_close_sma_1M, 
        LAG_close_sma_3M, 
        LAG_close_sma_6M, 
        LAG_close_sma_1Y, 
        LAG_close_short_ema, 
        LAG_close_long_ema, 
        LAG_close_vlong_ema, 
        LAG_high_sma_1W, 
        LAG_high_sma_1M, 
        LAG_high_sma_3M, 
        LAG_high_sma_6M, 
        LAG_high_sma_1Y, 
        LAG_high_short_ema, 
        LAG_high_long_ema, 
        LAG_high_vlong_ema, 
        LAG_low_sma_1W, 
        LAG_low_sma_1M, 
        LAG_low_sma_3M, 
        LAG_low_sma_6M, 
        LAG_low_sma_1Y, 
        LAG_low_short_ema, 
        LAG_low_long_ema, 
        LAG_low_vlong_ema, 
        LAG_volume_sma_1W, 
        LAG_volume_sma_1M, 
        LAG_volume_sma_3M, 
        LAG_volume_sma_6M, 
        LAG_volume_sma_1Y, 
        LAG_volume_short_ema, 
        LAG_volume_long_ema, 
        LAG_volume_vlong_ema
 FROM stocks.daily_prices_cvcbsa_training 
 ORDER BY symbol,
          LAG_volume_vlong_ema
UNSEGMENTED ALL NODES;

CREATE PROJECTION daily_prices_cvcbsa_simulation_DBD_4_rep_CMP01 /*+createtype(D)*/
(
 ts ENCODING COMMONDELTA_COMP, 
 symbol ENCODING RLE, 
 open ENCODING COMMONDELTA_COMP, 
 close ENCODING COMMONDELTA_COMP, 
 high ENCODING COMMONDELTA_COMP, 
 low ENCODING COMMONDELTA_COMP, 
 volume ENCODING AUTO
)
AS
 SELECT ts, 
        symbol, 
        open, 
        close, 
        high, 
        low, 
        volume
 FROM stocks.daily_prices_cvcbsa_simulation 
 ORDER BY symbol,
          ts
UNSEGMENTED ALL NODES;

CREATE PROJECTION daily_prices_cvcbsa_prediction_DBD_5_rep_CMP01 /*+createtype(D)*/
(
 ts ENCODING COMMONDELTA_COMP, 
 symbol ENCODING RLE, 
 open ENCODING COMMONDELTA_COMP, 
 close ENCODING COMMONDELTA_COMP, 
 high ENCODING COMMONDELTA_COMP, 
 low ENCODING COMMONDELTA_COMP, 
 volume ENCODING AUTO
)
AS
 SELECT ts, 
        symbol, 
        open, 
        close, 
        high, 
        low, 
        volume
 FROM stocks.daily_prices_cvcbsa_prediction 
 ORDER BY symbol,
          volume
UNSEGMENTED ALL NODES;

select refresh('stocks.daily_prices, stocks.stock_symbols, stocks.daily_prices_cvcbsa_training, stocks.daily_prices_cvcbsa_simulation, stocks.daily_prices_cvcbsa_prediction');

select make_ahm_now();

DROP PROJECTION stocks.daily_prices_super CASCADE;

DROP PROJECTION stocks.stock_symbols_super CASCADE;

DROP PROJECTION stocks.daily_prices_cvcbsa_training_super CASCADE;

DROP PROJECTION stocks.daily_prices_cvcbsa_simulation_super CASCADE;

DROP PROJECTION stocks.daily_prices_cvcbsa_prediction_super CASCADE;