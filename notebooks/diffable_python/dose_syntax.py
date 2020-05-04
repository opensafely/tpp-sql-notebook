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

# TEST on dose syntax

import pyodbc
import pandas as pd
from IPython.display import display, Markdown

server = 'covid.ebmdatalab.net,1433'
database = 'OPENCoronaExport' 
username = 'SA'
password = 'ahsjdkaJAMSHDA123[' 
cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()


# +
sql = "select dose from MedicationIssue"    
   
medissue = (pd.read_sql(sql, cnxn))
# -

#pd.set_option('display.max_rows', None)
#pd.set_option('display.max_colwidth', None)
medissue.head(10)

medissue.nunique()

medissue.count()

#medissue.groupby("dose").count()
count = medissue['dose'].value_counts()
count.head(10)

cnxn.close()

# MedicationIssue
