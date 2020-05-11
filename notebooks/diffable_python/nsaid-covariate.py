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

# # NSAID prescriptions
# - Long-term treatment
# - Acute treatment in previous n weeks

import os
import pyodbc
import pandas as pd
from IPython.display import display, Markdown
from ebmdatalab import bq

server = 'covid.ebmdatalab.net,1433'
database = 'OPENCoronaExport' 
username = 'SA'
password = 'ahsjdkaJAMSHDA123[' 
cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()

# ### Get BNF codes mapped to snomed codes
# - taken from https://github.com/ebmdatalab/nsaid-covid-codelist-notebook/blob/2432fc3bd714b4ba5aaaf8b724dff031faa52644/notebooks/nsaid.codelist.ipynb

# +
sql = '''WITH bnf_codes AS (
  SELECT bnf_code FROM hscic.presentation WHERE 
  bnf_code LIKE '1001010%' #bnf section non-steroidal anti-inflammatory drugs
  )

SELECT "vmp" AS type, id, bnf_code, nm
FROM dmd.vmp
WHERE bnf_code IN (SELECT * FROM bnf_codes)

UNION ALL

SELECT "amp" AS type, id, bnf_code, descr
FROM dmd.amp
WHERE bnf_code IN (SELECT * FROM bnf_codes)

ORDER BY type, bnf_code, id'''

nsaid_codelist = bq.cached_read(sql, csv_path=os.path.join('..','data','nsaid_codelist.csv'))
nsaid_codelist.head(10)


# -

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


codes_to_sql_where("DMD_ID", nsaid_codelist['id'].tail())

# ### Count NSAID prescribing events within specified time periods

# NBVAL_IGNORE_OUTPUT
codes_where = codes_to_sql_where("DMD_ID", nsaid_codelist['id'])
query = f'''
SELECT 
  med.Patient_ID,
  SUM(CASE WHEN (StartDate BETWEEN '2019-04-01' AND '2020-03-31') THEN 1 ELSE 0 END) AS nsaid_long_term,
  SUM(CASE WHEN (StartDate BETWEEN '2020-03-01' AND '2020-03-31') THEN 1 ELSE 0 END) AS nsaid_acute

FROM MedicationDictionary AS dict

INNER JOIN MedicationIssue AS med
ON dict.MultilexDrug_ID = med.MultilexDrug_ID

WHERE ({codes_where})

GROUP BY med.Patient_ID

ORDER BY Patient_ID
'''
df = pd.read_sql(query, cnxn, index_col='Patient_ID')
df.loc[df['nsaid_long_term']>0]

cnxn.close()
