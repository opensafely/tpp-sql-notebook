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
password = 'ahsjdkaJAMSHDA123[' 
cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()

qof_clusters = pd.read_csv('../data/QoFClusteres_CTV3Codes - Sheet1.csv')
chd_codes = qof_clusters.loc[qof_clusters['ClusterId']=='CHD','CTV3Code']
chd_codes.head()


def codes_to_sql_where(col_name, code_list):
    where = ""
    i = 0
    for code in code_list:
        if i == 0:
            where = where + f"{col_name} = '{code}'"
        else:
            where = where + f" OR {col_name} = '{code}'"
        i+=1
    return where


codes_to_sql_where("CTV3Code", chd_codes.head())

codes_where = codes_to_sql_where("CTV3Code", chd_codes)
query = f'''
SELECT Patient_ID
FROM CodedEvent
WHERE {codes_where}
'''
df = pd.read_sql(query, cnxn, index_col='Patient_ID')
df['chd'] = True
df
