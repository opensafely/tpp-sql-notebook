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

# # TPP Schema
#
# This notebook describes the schema of the TPP database, based on the dummy data supplied by TPP.
# Note that although the schema accurately reflects what's in production, the dummy data itself
# should not be considered in any sense representative.
#
# TPP upload new dumps of dummy data via SFTP to `covid.ebmdatalab.net`. To load a new data dump:
# ```
# ssh covid.ebmdatalab.net
# sudo -s
# # cd /home/sftpuser
# python3.8 restore.py OPENCorona_Test_Data_20200605.bak
# ```
# (Obviously using the filename of the latest backup instead.)

# ## Data from 2020-07-17
#
# Using file `OPENCorona_Test_Data_20200717.bak`

import pyodbc
import pandas as pd
from IPython.display import display, Markdown

server = 'covid.ebmdatalab.net,1433'
database = 'OPENCoronaExport' 
username = 'SA'
password = 'ahsjdkaJAMSHDA123[' 
cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()

pd.set_option('display.max_columns', None)

# select command
query = '''select name from sys.objects where type_desc='USER_TABLE' order by name'''
df = pd.read_sql(query, cnxn)
df

for table in df['name']:
    sql = f"select TOP 10  * from {table}"
    display(Markdown(f"## {table}"))
    display(pd.read_sql(sql, cnxn).head())

display(Markdown(f"## DataDictionary (full table)"))
display(pd.read_sql("select  * from DataDictionary", cnxn)
