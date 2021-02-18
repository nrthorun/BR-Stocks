# -*- coding: utf-8 -*-
"""
Script desenvolvido para obter dados de ações da B3 pelo yahooquery e inserir no SQL Server

Autor: Nikolas Thorun
"""

import pandas as pd
import pyodbc
from yahooquery import Ticker

##Lista os drivers disponíveis
#pyodbc.drivers()

## Connection string
conn = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};"
                    "TrustServerCertificate=No;"
                    "WSID= {Your PC Name Here};"
                    "APP=Microsoft® Windows® Operating System;"
                    "Trusted_Connection=Yes;"
                    "SERVER= {Your Server Name Here};"
)

## Símbolos das ações B3 de acordo com o Yahoo Finance
ticker_list = [
      'AZUL4.SA',	'GOLL4.SA',	'CSAN3.SA',	'SMTO3.SA',	'AGRO3.SA',	'SLCE3.SA',	'ABEV3.SA',	'BEEF3.SA',	
      'BRFS3.SA',	'CAML3.SA',	'JBSS3.SA',	'MDIA3.SA',	'MRFG3.SA',	'ABCB4.SA',	'BBAS3.SA',	'BBDC4.SA',	
      'BIDI11.SA',	'BMGB4.SA',	'BPAC11.SA','BPAN4.SA',	'BRSR6.SA',	'ITUB4.SA',	'ITSA4.SA',	'SANB11.SA',	
      'CCRO3.SA',	'ECOR3.SA',	'CURY3.SA',	'CYRE3.SA',	'DIRR3.SA',	'EVEN3.SA',	'EZTC3.SA',	'LAVV3.SA',	
      'MELK3.SA',	'MDNE3.SA',	'MRVE3.SA',	'MTRE3.SA',	'PLPL3.SA',	'TEND3.SA',	'TRIS3.SA',	'ANIM3.SA',	
      'COGN3.SA',	'SEER3.SA',	'YDUQ3.SA',	'CMIG4.SA',	'CPLE6.SA',	'EGIE3.SA',	'ENBR3.SA',	'LIGT3.SA',	
      'NEOE3.SA',	'TIET11.SA','TRPL4.SA',	'BOAS3.SA',	'B3SA3.SA',	'CIEL3.SA',	'CASH3.SA',	'AERI3.SA',	
      'EMBR3.SA',	'TASA4.SA',	'FRAS3.SA',	'LEVE3.SA',	'MYPK3.SA',	'POMO4.SA',	'RAPT4.SA',	'ROMI3.SA',	
      'PRNR3.SA',	'TUPY3.SA',	'WEGE3.SA',	'AMBP3.SA',	'BRKM5.SA',	'UNIP6.SA',	'LCAM3.SA',	'MOVI3.SA',	
      'RENT3.SA',	'HBSA3.SA',	'JSLG3.SA',	'LOGN3.SA',	'SEQL3.SA',	'SIMH3.SA',	'STBP3.SA',	'RAIL3.SA',	
      'TGMA3.SA',	'EUCA4.SA',	'PTBL3.SA',	'BRAP4.SA',	'FESA4.SA',	'GGBR4.SA',	'GOAU4.SA',	'USIM5.SA',	
      'VALE3.SA',	'BRDT3.SA',	'PETR3.SA',	'PETR4.SA',	'PRIO3.SA',	'ENAT3.SA',	'UGPA3.SA',	'IVVB11.SA',
      'RRRP3.SA',	'RANI3.SA',	'KLBN11.SA','SUZB3.SA',	'SMLS3.SA',	'ALPK3.SA',	'LOGG3.SA',	'SCAR3.SA',	
      'IRBR3.SA',	'CSMG3.SA',	'SAPR11.SA','AALR3.SA',	'DMVF3.SA',	'FLRY3.SA',	'GNDI3.SA',	'HAPV3.SA',	
      'HYPE3.SA',	'ODPV3.SA',	'PARD3.SA',	'PFRM3.SA',	'PGMN3.SA',	'PNVL3.SA',	'RADL3.SA',	'RDOR3.SA',	
      'BBSE3.SA',	'PSSA3.SA',	'SULA11.SA','ALSO3.SA',	'IGTA3.SA',	'MULT3.SA',	'CARD3.SA',	'LINX3.SA',	
      'LWSA3.SA',	'NGRD3.SA',	'POSI3.SA',	'SQIA3.SA',	'TOTS3.SA',	'VLID3.SA',	'TIMS3.SA',	'VIVT3.SA',	
      'ARZZ3.SA',	'BTOW3.SA',	'BKBR3.SA',	'CEAB3.SA',	'CNTO3.SA',	'CRFB3.SA',	'CVCB3.SA',	'ENJU3.SA',	
      'PCAR3.SA',	'HGTX3.SA',	'LAME4.SA',	'LJQQ3.SA',	'LREN3.SA',	'MGLU3.SA',	'NTCO3.SA',	'PETZ3.SA',	
      'GMAT3.SA',	'SOMA3.SA',	'TFCO4.SA',	'VULC3.SA',	'VIVA3.SA',	'VVAR3.SA',	'GGBR3.SA',	'BOVV11.SA',	
      'RBRF11.SA',  'RBRR11.SA','HGRU11.SA','IRDM11.SA'
        ]

## Declarando a lista final de ações
stock_summary = pd.DataFrame()

## Itera pela lista de símbolos pegando dados h'istoricos do período definido abaixo
for i in ticker_list:
    get_ticker = Ticker(i)
    ticker_history = pd.DataFrame(get_ticker.history(start='2019-01-01', end='2021-02-01'))
    stock_summary = stock_summary.append(ticker_history, ignore_index=False)

## Transforma o index nas colunas symbol e date. Deleta as colunas dividends e splits - Removidas por ausência de dados
stock_summary.reset_index(inplace=True) 
stock_summary = stock_summary.drop(axis=1, labels=['dividends','splits'])

## Conecta no SQL Server e insere a lista de ações
cursor = conn.cursor()
cursor.executemany("""
                  INSERT INTO [PROD].[dbo].[stocks]
                  (ticker, date, adjclose, closed, high, low, opened, volume,insert_dt)
                  VALUES (?,?,?,?,?,?,?,?,GETDATE())
                  """, stock_summary.values.tolist())
conn.commit()

## Fecha a conexão com o SQL Server
cursor.close()
conn.close()
