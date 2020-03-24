# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: all
#     notebook_metadata_filter: all,-language_info
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.3.3
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

import pyodbc
import pandas as pd

# +
# enter ip address and port number of the system where the database resides.
#sqlcmd -S covid.ebmdatalab.net,1433 -U SA -P "$SQLPW" -q "EXIT(SELECT TOP 10 StockItemID, StockItemName FROM WideWorldImporters.Warehouse.StockItems ORDER BY StockItemID)"

server = 'covid.ebmdatalab.net,1433'
database = 'WideWorldImporters' # enter database name
username = 'SA'
password = 'ahsjdkaJAMSHDA123[' # add appropriate driver name
cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()
# -

# select command
query = ''' SELECT TOP 10 StockItemID, StockItemName FROM WideWorldImporters.Warehouse.StockItems ORDER BY StockItemID'''
data = pd.read_sql(query, cnxn)
data.head()
