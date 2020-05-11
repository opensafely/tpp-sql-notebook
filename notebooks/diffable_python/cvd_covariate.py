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

import os
import pyodbc
import pandas as pd
from IPython.display import display, Markdown
from ebmdatalab import bq

# ### Server connection

server = 'covid.ebmdatalab.net,1433'
database = 'OPENCoronaExport' 
username = 'SA'
password = 'ahsjdkaJAMSHDA123[' 
cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()

# ### CHD codes
# - just those in the CHD cluster, to identify cardiac events
# - in order to differentiate between patients on primary and secondary prevention

# NBVAL_IGNORE_OUTPUT
qof_clusters = pd.read_csv('../data/QoFClusteres_CTV3Codes - Sheet1.csv')
chd_codes = qof_clusters.loc[qof_clusters['ClusterId']=='CHD','CTV3Code']
chd_codes.head()

# ### CVD medicine codes
# - taken from https://github.com/ebmdatalab/cvd-covid-codelist-notebook/blob/master/notebooks/cvd.codelist.ipynb

# +
# NBVAL_IGNORE_OUTPUT
sql = '''
WITH bnf_codes AS (
  SELECT bnf_code FROM hscic.presentation WHERE 
    bnf_code LIKE '02%' #BNF cvd chapter 
)

SELECT "vmp" AS type, id, bnf_code, nm
FROM dmd.vmp
WHERE bnf_code IN (SELECT * FROM bnf_codes)

UNION ALL

SELECT "amp" AS type, id, bnf_code, descr
FROM dmd.amp
WHERE bnf_code IN (SELECT * FROM bnf_codes)

ORDER BY type, bnf_code, id
'''

cvd_medcodes = bq.cached_read(sql, csv_path=os.path.join('..','data','cvd_medcodes.csv'))
cvd_medcodes.head()


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


# NBVAL_IGNORE_OUTPUT
codes_to_sql_where("CTV3Code", chd_codes.head())

# NBVAL_IGNORE_OUTPUT
codes_where = codes_to_sql_where("CTV3Code", chd_codes)
query = f'''
SELECT DISTINCT Patient_ID, 1 AS chd_code
FROM CodedEvent
WHERE {codes_where}
ORDER BY Patient_ID
'''
clin_df = pd.read_sql(query, cnxn, index_col='Patient_ID')
clin_df

# +
# NBVAL_IGNORE_OUTPUT

codes_where = codes_to_sql_where("DMD_ID", cvd_medcodes['id'])
query = f'''
SELECT 
  med.Patient_ID,
  COUNT(med.Patient_ID) AS cvd_meds

FROM MedicationDictionary AS dict

INNER JOIN MedicationIssue AS med
ON dict.MultilexDrug_ID = med.MultilexDrug_ID

WHERE ({codes_where})

GROUP BY med.Patient_ID

ORDER BY med.Patient_ID
'''
med_df = pd.read_sql(query, cnxn, index_col='Patient_ID')
med_df.head()
# -

# NBVAL_IGNORE_OUTPUT
clin_df.join(med_df, how='outer').fillna(0).head()

# ### Connection should be closed before restarting the kernal or closing the notebook

cnxn.close()
