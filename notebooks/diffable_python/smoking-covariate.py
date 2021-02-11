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

# ### Import statements

import pyodbc
import pandas as pd
from IPython.display import display, Markdown

# ### Connect to server with dummy data

server = 'covid.ebmdatalab.net,1433'
database = 'OPENCoronaExport' 
username = 'SA'
password = 'my_secret_password' 
cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()

# ### Find the smoking codes from QoF

qof = pd.read_csv('../data/QoFClusteres_CTV3Codes - Sheet1.csv')

# #### Import function

from lib.data_gathering import search_terms_to_df

# ### Search Qof codes

smoking_list = ['SMOK', 'CESS']

smoking_qof = search_terms_to_df(smoking_list, col_name='ClusterId', df_name=qof)

smoking_qof.head()

# #### Output codes to csv

smoking_qof.to_csv('../data/smoking_codes.csv')

# ### Checking
#
# There would need to be some sort of manual checking of these codes

# ### Find people with these codes in TPP data

from lib.data_gathering import codes_to_sql_where

codes_where = codes_to_sql_where("CTV3Code", smoking_qof['CTV3Code'])

query = f'''
SELECT * --DISTINCT Patient_ID
FROM CodedEvent
WHERE ({codes_where}) AND ConsultationDate BETWEEN '2015-01-01' AND '2020-03-31'
'''
df = pd.read_sql(query, cnxn, index_col='Patient_ID')
df.head()

cnxn.close()


