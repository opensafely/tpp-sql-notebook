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

# ### Import Statements

import os
import pyodbc
import pandas as pd
from IPython.display import display, Markdown
from ebmdatalab import bq
import datalab_covariates as dlc

# ### Server connection

server = 'covid.ebmdatalab.net,1433'
database = 'OPENCorona'
username = 'SA'
password = 'ahsjdkaJAMSHDA123[' 
cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()
dlc.set_backend(
    "tpp",
    hostname='covid.ebmdatalab.net',
    port='1433',
    database='OPENCoronaExport' ,
    username='SA',
    password='ahsjdkaJAMSHDA123[',
)

# ### Population 
#
# Our study population is everyone in CHESS (dummy) dataset that had a positive covid-19 swab

chess = pd.read_csv('../data/chess.csv')
chess.head()

study_population = chess[chess['result'] == 'COVID-19']

study_population.head()

study_population.to_csv("../data/analysis/study_pop.csv")

# ### Outcome and Exposure definitions

# #### Exposure: CVD disease ever
#
# In this analysis, our exposure is Cardiovascular disease. We define this as a code on a patient's records ever that is for cardiovascular disease such as angina or a prescription of a cardiovascular drugs (from bnf)

# ##### Read codes

qof_clusters = pd.read_csv('../data/QoFClusteres_CTV3Codes - Sheet1.csv')
chd_codes = qof_clusters.loc[qof_clusters['ClusterId']=='CHD','CTV3Code']
chd_codes.head()

# ##### Medicine

# +
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

# #### Find patients with coded events where code matches cvd disease

chd_patients = dlc.patients_with_these_clinical_events("chd_code", ctv3_codes=chd_codes)
clin_df = chd_patients.to_df()
clin_df.head()

# #### Find all patients where cvd code on record

# +
cvd_patients = dlc.patients_with_these_medications('cvd_meds', snomed_codes=cvd_medcodes['id'])

med_df = cvd_patients.to_df()
med_df.head()
# -

clin_df.join(med_df, how='outer').fillna(0).head()

clin_df.to_csv("../data/analysis/cvd_dis.csv")

# #### Outcome: Death
#
# Our outcome of interest is death
#
# 1 - death
# 0 - alive
#
# This is contained within the study pop csv

# ### Covariates defintions
#
# - Age
# - Gender
# - Smoking status

df = pd.read_csv('../data/analysis/study_pop.csv')
df.rename({'Patient_ID': 'patient_id'}, axis=1, inplace=True)

df.head()

# #### Demographics 

demographics = dlc.patients_with_age_and_sex('2020-04-01', patient_ids=df['patient_id'])
co_df = demographics.to_df()
co_df.head()

co_df.to_csv('../data/analysis/demo.csv')

# #### Smoking status

smoking_stat = pd.read_csv('../data/smoking_codes.csv')

smoking_stat.head()

# +
smokers = dlc.patients_with_these_clinical_events(
    "smoking_status",
    ctv3_codes=smoking_stat['CTV3Code'],
    min_date='2015-01-01',
    max_date='2020-03-31'
)

smok_df = smokers.to_df()
smok_df.head()
# -

smok_df.to_csv('../data/analysis/smok.csv')

# ### Making final dataset 

study_pop = pd.read_csv('../data/analysis/study_pop.csv')

study_pop = study_pop[['Patient_ID', 'admitted_itu', 'died']]
study_pop.rename({'Patient_ID': 'patient_id'}, axis=1, inplace=True)

study_pop.head()

# ##### Add in exposure

cvd = pd.read_csv('../data/analysis/cvd_dis.csv')

cvd.head()

study_pop = study_pop.merge(cvd, how='left', on='patient_id')

study_pop.fillna(0, inplace=True)

study_pop.head()

# ##### Add in demographics and other covariates

demo = pd.read_csv('../data/analysis/demo.csv')

demo = demo[['patient_id', 'sex', 'age']]

demo.head()

study_pop = study_pop.merge(demo, how='left', on='patient_id')

study_pop.head()

# ##### Add in smoking

smok = pd.read_csv('../data/analysis/smok.csv')

smok = smok[['patient_id', 'smoking_status']]

smok.head()

study_pop = study_pop.merge(smok, how='left', on='patient_id')
study_pop.fillna(0, inplace=True)

study_pop.head()

study_pop.to_csv('../data/analysis/final_dataset.csv')
