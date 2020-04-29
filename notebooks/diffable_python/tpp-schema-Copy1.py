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

# NBVAL_IGNORE_OUTPUT
# select command
query = '''select name from sys.objects where type_desc='USER_TABLE' order by name'''
df = pd.read_sql(query, cnxn)
df

# NBVAL_IGNORE_OUTPUT
for table in df['name']:
    sql = f"select TOP 10  * from {table}"
    display(Markdown(f"## {table}"))
    display(pd.read_sql(sql, cnxn).head())

# ## SQL query
#
# In this case join patient table to clinical event table where the clinical event is Pneumonia and event date was later than 01/01/2005/ 
#

sql = "SELECT * FROM CodedEvent INNER JOIN Patient ON CodedEvent.Patient_ID=Patient.Patient_ID WHERE SnomedConceptId='233604007' AND ConsultationDate>'2005-01-01 00:00:00'"

# #### Run the query

cohort = pd.read_sql(sql, cnxn)

# #### Display Results

# NBVAL_IGNORE_OUTPUT
cohort.head()


