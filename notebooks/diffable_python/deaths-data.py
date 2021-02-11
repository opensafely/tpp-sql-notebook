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
from IPython.display import display, Markdown

server = 'covid.ebmdatalab.net,1433'
database = 'OPENCoronaExport' 
username = 'SA'
password = 'my_secret_password' 
cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()

pd.read_sql("select top 10 * from CPNS", cnxn).iloc[0]

pd.read_sql("select top 10 * from SGSS_Negative", cnxn).iloc[0]

pd.read_sql("select top 10 * from SGSS_Positive", cnxn).iloc[0]

pd.read_sql("select top 10 * from ONS_Deaths", cnxn).iloc[0]

