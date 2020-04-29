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

# # Vaccination status
# Adults 
# - Pneumococcal
# - Flu vaccine (time-sensitive: within current flu season)
# - Shingles
#
#
# Children 
# See NHS website
#
#
# Vaccinations are recorded in two places:
# 1. A vaccination table (not in the current build of dummy data, needs adding in later)
# 2. Read codes in the `CodedEvent` table
#

import pyodbc
import pandas as pd
from IPython.display import display, Markdown

# ### Server connection

server = 'covid.ebmdatalab.net,1433'
database = 'OPENCoronaExport' 
username = 'SA'
password = 'ahsjdkaJAMSHDA123[' 
cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()

# ### Flu codes
# - Haphazardly taken from here, for speed: https://clinicalcodes.rss.mhs.man.ac.uk/medcodes/article/6/codelist/influenza-immunisation/
# - Looks like there's a completely different set in `../data/QoFClusteres_CTV3Codes - Sheet1.csv`

flu_read_codes = pd.read_csv('../data/flu_codes.csv')
flu_read_codes.head()


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


codes_to_sql_where("CTV3Code", flu_read_codes['Code'].head())

# ### Medcode based vaccinations
# - there are no values for the current flu season in the dummy data, but it works for previous time periods

# NBVAL_IGNORE_OUTPUT
codes_where = codes_to_sql_where("CTV3Code", flu_read_codes['Code'])
query = f'''
SELECT * --DISTINCT Patient_ID
FROM CodedEvent
WHERE ({codes_where}) AND ConsultationDate BETWEEN '2019-09-01' AND '2020-03-31'
'''
df = pd.read_sql(query, cnxn, index_col='Patient_ID')
df

# ### Connection should be closed before restarting the kernal or closing the notebook

cnxn.close()
