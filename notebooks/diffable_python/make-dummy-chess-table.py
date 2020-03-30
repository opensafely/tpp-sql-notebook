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

# # Create dummy CHESS table
# - While we are waiting for the real thing.
# - Fields are taken from:
# https://www.england.nhs.uk/coronavirus/wp-content/uploads/sites/52/2020/03/phe-letter-to-trusts-re-daily-covid-19-hospital-surveillance-11-march-2020.pdf
# - Rather than try to replicate the duplication/missingness/chaos of the raw table, I've just created what I imagine is a tidied up version with a handful of useful fields for cohort selection.
# - We can add to these fields as needed.

import pyodbc
import pandas as pd
from IPython.display import display, Markdown

server = 'covid.ebmdatalab.net,1433'
database = 'OPENCoronaExport' 
username = 'SA'
password = 'ahsjdkaJAMSHDA123[' 
cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()

# select command
query = '''select name from sys.objects where type_desc='USER_TABLE' order by name'''
df = pd.read_sql(query, cnxn)
df

# ## Get random sample of patient ids from TPP data

sql = f"select * from Patient"
patient = pd.read_sql(sql, cnxn).set_index('Patient_ID')
rand_samp = patient.sample(n=1000,random_state=1234).index
rand_samp

# ## Make random dates

import numpy as np
from datetime import datetime, date
def random_dates(start, end, n, unit='D', seed=None):
    if seed:
        np.random.seed(seed)
    else:
        np.random.seed(0)

    ndays = (end - start).days + 1
    return start + pd.to_timedelta(
        np.random.randint(0, ndays, n), unit=unit
    )


start = datetime(2020, 2, 1, 0, 0)
end = datetime.combine(date.today(), datetime.min.time())

# ## Make table and columns
# - Estimated date of onset of symptoms
# - Swab/specimen date
# - Laboratory test date
# - Result of laboratory tests (select all that apply): COVID-19, A/H1N1pdm2009, A/H3N2, B, A/non-subtyped, A/unsubtypeable, RSV, other (specify)

# +
results = ['COVID-19', 'COVID-19', 'COVID-19', 'COVID-19', 'COVID-19', 'COVID-19', 'COVID-19', #so it's mostly covid
           'A/H1N1pdm2009',
           'A/H3N2',
           'B',
           'A/non-subtyped',
           'A/unsubtypeable',
           'RSV',
           'other (specify)'
          ]

chess = pd.DataFrame(index= rand_samp)
chess['symptom_onset'] = random_dates(start, end, len(chess),seed=123)
chess['swab_date'] = random_dates(start, end, len(chess),seed=321)
chess['lab_test_date'] = random_dates(start, end, len(chess),seed=321)
chess['result'] = np.random.randint(0, len(results), len(chess))
chess['result'] = chess['result'].apply(lambda i: results[i])
# -

chess.head()

# #### Make outcome data

chess['admitted_itu'] = np.random.choice([0, 1], size=(len(chess)), p=[8./10, 2./10])
chess['admission_date'] = pd.to_datetime(np.NaN)
chess.loc[chess['admitted_itu']==1,'admission_date'] = chess.loc[chess['admitted_itu']==1,'swab_date'] + pd.DateOffset(days=10)
chess['died'] = np.random.choice([0, 1], size=(len(chess)), p=[9./10, 1./10])
chess['death_date'] = pd.to_datetime(np.NaN)
chess.loc[chess['died']==1,'death_date'] = chess.loc[chess['died']==1,'swab_date'] + pd.DateOffset(days=20)

chess.tail()

chess.to_csv('../data/chess.csv')
