ALTER DATABASE stocks SET MaxClientSessions = 1000;
ALTER DATABASE stocks SET DivideZeroByZeroThrowsError = 0;

--drop schema stocks cascade;
create schema stocks;

CREATE TABLE IF NOT EXISTS stocks.daily_prices 
(
    ts DATETIME, 
    symbol VARCHAR(15), 
    open NUMERIC(12,2), 
    high NUMERIC(12,2), 
    low NUMERIC(12,2), 
    close NUMERIC(12,2), 
    volume NUMERIC(20) 
);

drop table if exists stocks.stock_symbols;
CREATE TABLE IF NOT EXISTS stocks.stock_symbols 
(
    company VARCHAR(50), 
    symbol VARCHAR(10), 
    industry VARCHAR(100), 
    headquarters VARCHAR(100) 
);

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

INSERT INTO stocks.stock_symbols VALUES ('AmBev', 'ABEV3.SA', 'beverages', 'São Paulo');
INSERT INTO stocks.stock_symbols VALUES ('Azul', 'AZUL4.SA', 'airlines', 'Barueri');
INSERT INTO stocks.stock_symbols VALUES ('B2W', 'BTOW3.SA', 'online retail', 'Rio de Janeiro');
INSERT INTO stocks.stock_symbols VALUES ('B3', 'B3SA3.SA', 'stock exchange', 'São Paulo');
INSERT INTO stocks.stock_symbols VALUES ('Banco do Brasil', 'BBAS3.SA', 'banking', 'Brasília');
INSERT INTO stocks.stock_symbols VALUES ('BB Seguridade', 'BBSE3.SA', 'insurance', 'Brasília');
INSERT INTO stocks.stock_symbols VALUES ('Bradesco', 'BBDC3.SA', 'banking', 'Osasco');
INSERT INTO stocks.stock_symbols VALUES ('Bradesco', 'BBDC4.SA', 'banking', 'Osasco');
INSERT INTO stocks.stock_symbols VALUES ('Bradespar', 'BRAP4.SA', 'holding', 'São Paulo');
INSERT INTO stocks.stock_symbols VALUES ('BRMalls', 'BRML3.SA', 'real state', 'Rio de Janeiro');
INSERT INTO stocks.stock_symbols VALUES ('Braskem', 'BRKM5.SA', 'petrochemicals', 'São Paulo');
INSERT INTO stocks.stock_symbols VALUES ('BRF', 'BRFS3.SA', 'foods', 'Itajaí');
INSERT INTO stocks.stock_symbols VALUES ('BTG Pactual', 'BPAC11.SA', 'banking', 'São Paulo');
INSERT INTO stocks.stock_symbols VALUES ('Carrefour Brasil', 'CRFB3.SA', 'retail', 'São Paulo');
INSERT INTO stocks.stock_symbols VALUES ('CCR', 'CCRO3.SA', 'transportation', 'São Paulo');
INSERT INTO stocks.stock_symbols VALUES ('CVC Brasil', 'CVCB3.SA', 'travel and tourism', 'Santo André');
INSERT INTO stocks.stock_symbols VALUES ('CEMIG', 'CMIG4.SA', 'electricity utility', 'Belo Horizonte');
INSERT INTO stocks.stock_symbols VALUES ('Cia. Hering', 'HGTX3.SA', 'clothing', 'Blumenau');
INSERT INTO stocks.stock_symbols VALUES ('Cielo', 'CIEL3.SA', 'payment system', 'Barueri');
INSERT INTO stocks.stock_symbols VALUES ('Cogna', 'COGN3.SA', 'higher education', 'Belo Horizonte');
INSERT INTO stocks.stock_symbols VALUES ('Cosan', 'CSAN3.SA', 'conglomerate', 'São Paulo');
INSERT INTO stocks.stock_symbols VALUES ('CPFL Energia', 'CPFE3.SA', 'electric utility', 'Campinas');
INSERT INTO stocks.stock_symbols VALUES ('CSN', 'CSNA3.SA', 'siderurgy and metallurgy', 'Rio de Janeiro');
INSERT INTO stocks.stock_symbols VALUES ('Cyrela Brazil Realty', 'CYRE3.SA', 'real estate', 'São Paulo');
INSERT INTO stocks.stock_symbols VALUES ('EcoRodovias', 'ECOR3.SA', 'transportation', 'São Paulo');
INSERT INTO stocks.stock_symbols VALUES ('EDP - Energias do Brasil', 'ENBR3.SA', 'electricity utility', 'São Paulo');
INSERT INTO stocks.stock_symbols VALUES ('Eletrobras', 'ELET3.SA', 'electric utility', 'Rio de Janeiro');
INSERT INTO stocks.stock_symbols VALUES ('Eletrobras', 'ELET6.SA', 'electric utility', 'Rio de Janeiro');
INSERT INTO stocks.stock_symbols VALUES ('Embraer', 'EMBR3.SA', 'aerospace/defense', 'São José dos Campos');
INSERT INTO stocks.stock_symbols VALUES ('ENGIE Brasil', 'EGIE3.SA', 'electricity utility', 'Florianópolis');
INSERT INTO stocks.stock_symbols VALUES ('Equatorial Energia', 'EQTL3.SA', 'electricity utility', 'Brasília');
INSERT INTO stocks.stock_symbols VALUES ('Gerdau', 'GGBR4.SA', 'siderurgy and metallurgy', 'São Paulo');
INSERT INTO stocks.stock_symbols VALUES ('Gol', 'GOLL4.SA', 'airlines', 'São Paulo');
INSERT INTO stocks.stock_symbols VALUES ('GPA', 'PCAR4.SA', 'retail', 'São Paulo');
INSERT INTO stocks.stock_symbols VALUES ('Grupo Fleury', 'FLRY3.SA', 'healthcare', 'São Paulo');
INSERT INTO stocks.stock_symbols VALUES ('Grupo Hapvida', 'HAPV3.SA', 'healthcare', 'Fortaleza');
INSERT INTO stocks.stock_symbols VALUES ('Hypera Pharma', 'HYPE3.SA', 'pharmaceutical', 'São Paulo');
INSERT INTO stocks.stock_symbols VALUES ('Iguatemi', 'IGTA3.SA', 'shopping malls', 'São Paulo');
INSERT INTO stocks.stock_symbols VALUES ('Intermédica', 'GNDI3.SA', 'healthcare', 'São Paulo');
INSERT INTO stocks.stock_symbols VALUES ('IRB Brasil RE', 'IRBR3.SA', 'insurance', 'Rio de Janeiro');
INSERT INTO stocks.stock_symbols VALUES ('Itaú Unibanco', 'ITUB4.SA', 'banking', 'São Paulo');
INSERT INTO stocks.stock_symbols VALUES ('Itaúsa', 'ITSA4.SA', 'holding', 'São Paulo');
INSERT INTO stocks.stock_symbols VALUES ('JBS', 'JBSS3.SA', 'food and beverages', 'São Paulo');
INSERT INTO stocks.stock_symbols VALUES ('Klabin', 'KLBN11.SA', 'paper and pulp', 'São Paulo');
INSERT INTO stocks.stock_symbols VALUES ('Localiza', 'RENT3.SA', 'rental car', 'Belo Horizonte');
INSERT INTO stocks.stock_symbols VALUES ('Lojas Americanas', 'LAME4.SA', 'department store', 'Rio de Janeiro');
INSERT INTO stocks.stock_symbols VALUES ('Lojas Renner', 'LREN3.SA', 'department store', 'Porto Alegre');
INSERT INTO stocks.stock_symbols VALUES ('Magazine Luiza', 'MGLU3.SA', 'department store', 'São Paulo');
INSERT INTO stocks.stock_symbols VALUES ('Marfrig', 'MRFG3.SA', 'foods', 'São Paulo');
INSERT INTO stocks.stock_symbols VALUES ('Metalúrgica Gerdau', 'GOAU4.SA', 'holding', 'Porto Alegre');
INSERT INTO stocks.stock_symbols VALUES ('Minerva Foods', 'BEEF3.SA', 'foods', 'Barretos');
INSERT INTO stocks.stock_symbols VALUES ('MRV', 'MRVE3.SA', 'construction and real estate', 'Belo Horizonte');
INSERT INTO stocks.stock_symbols VALUES ('Multiplan', 'MULT3.SA', 'shopping malls', 'Rio de Janeiro');
INSERT INTO stocks.stock_symbols VALUES ('Natura & Co', 'NTCO3.SA', 'cosmetics', 'São Paulo');
INSERT INTO stocks.stock_symbols VALUES ('Petrobras', 'PETR3.SA', 'oil and gas', 'Rio de Janeiro');
INSERT INTO stocks.stock_symbols VALUES ('Petrobras', 'PETR4.SA', 'oil and gas', 'Rio de Janeiro');
INSERT INTO stocks.stock_symbols VALUES ('Qualicorp', 'QUAL3.SA', 'insurance', 'São Paulo');
INSERT INTO stocks.stock_symbols VALUES ('RaiaDrogasil', 'RADL3.SA', 'drugstore', 'São Paulo');
INSERT INTO stocks.stock_symbols VALUES ('Rumo', 'RAIL3.SA', 'logistics', 'Curitiba');
INSERT INTO stocks.stock_symbols VALUES ('Sabesp', 'SBSP3.SA', 'waste management', 'São Paulo');
INSERT INTO stocks.stock_symbols VALUES ('Santander Brasil', 'SANB11.SA', 'banking', 'São Paulo');
INSERT INTO stocks.stock_symbols VALUES ('SulAmérica Seguros', 'SULA11.SA', 'insurance', 'Rio de Janeiro');
INSERT INTO stocks.stock_symbols VALUES ('Suzano Papel e Celulose', 'SUZB3.SA', 'pulp and paper', 'Salvador');
INSERT INTO stocks.stock_symbols VALUES ('Taesa S.A.', 'TAEE11.SA', 'electricity utility', 'Rio de Janeiro');
INSERT INTO stocks.stock_symbols VALUES ('Telefônica Vivo', 'VIVT4.SA', 'telecommunications', 'São Paulo');
INSERT INTO stocks.stock_symbols VALUES ('TIM Participações', 'TIMP3.SA', 'telecommunications', 'Rio de Janeiro');
INSERT INTO stocks.stock_symbols VALUES ('TOTVS', 'TOTS3.SA', 'software', 'São Paulo');
INSERT INTO stocks.stock_symbols VALUES ('Ultrapar', 'UGPA3.SA', 'conglomerate', 'São Paulo');
INSERT INTO stocks.stock_symbols VALUES ('Usiminas', 'USIM5.SA', 'siderurgy and metallurgy', 'Belo Horizonte');
INSERT INTO stocks.stock_symbols VALUES ('Vale', 'VALE3.SA', 'mining', 'Rio de Janeiro');
INSERT INTO stocks.stock_symbols VALUES ('Via Varejo', 'VVAR3.SA', 'retail', 'São Caetano do Sul');
INSERT INTO stocks.stock_symbols VALUES ('YDUQS', 'YDUQ3.SA', 'higher education', 'Rio de Janeiro');
INSERT INTO stocks.stock_symbols VALUES ('WEG', 'WEGE3.SA', 'industrial engineering', 'Jaraguá do Sul');

INSERT INTO stocks.stock_symbols VALUES ('Índice Bovespa', '^BVSP', 'index', 'São Paulo');
INSERT INTO stocks.stock_symbols VALUES ('Índice Valor BM&F Bovespa', '^IVBX', 'index', 'São Paulo');
INSERT INTO stocks.stock_symbols VALUES ('Índice Brasil 50', 'IBXX.SA', 'index', 'São Paulo');
INSERT INTO stocks.stock_symbols VALUES ('Índice Brasil Amplo', 'IBRA.SA', 'index', 'São Paulo');
INSERT INTO stocks.stock_symbols VALUES ('Índice Mid-Large Cap', 'MLCX.SA', 'index', 'São Paulo');
INSERT INTO stocks.stock_symbols VALUES ('Índice Small Cap', 'SMLL.SA', 'index', 'São Paulo');
INSERT INTO stocks.stock_symbols VALUES ('Índice de Dividendos', 'IDIV.SA', 'index', 'São Paulo');
INSERT INTO stocks.stock_symbols VALUES ('Índice de Energia Elétrica', '^IEE', 'index', 'São Paulo');
INSERT INTO stocks.stock_symbols VALUES ('Índice do Setor Industrial', 'INDX.SA', 'index', 'São Paulo');
INSERT INTO stocks.stock_symbols VALUES ('Índice de Consumo', 'ICON.SA', 'index', 'São Paulo');
INSERT INTO stocks.stock_symbols VALUES ('Índice Imobiliário', 'IMOB.SA', 'index', 'São Paulo');
INSERT INTO stocks.stock_symbols VALUES ('Índice Financeiro', 'IFNC.SA', 'index', 'São Paulo');
INSERT INTO stocks.stock_symbols VALUES ('Índice de Materiais Básicos', 'IMAT.SA', 'index', 'São Paulo');
INSERT INTO stocks.stock_symbols VALUES ('Índice de Utilidade Pública', 'UTIL.SA', 'index', 'São Paulo');