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

# select command
query = '''select name from sys.objects where type_desc='USER_TABLE' order by name'''
df = pd.read_sql(query, cnxn)
df

# +
desc = pd.DataFrame([["Appointment", "Primary care appointment dates"],
["CodedEvent", "Recorded events in primary care, e.g. prescriptions, referrals, diagnoses"],
["CodedEventRange", "Possible value ranges for events in `coded event` table"],
["Consultation", "Primary care consultation dates and IDs"],
["CPNS", "Daily COVID-related in-hospital deaths (may be incomplete)"],
["DataDictionary","Translation for codes present in TPP Data"],
["ECDS","A&E presentations from Emergency Care Data Set (ECDS)"],
["ECDS_EC_Diagnoses","Diagnosis codes required to extract relevant A&E presentations from ECDS"],
["EthnicityCodedEvent","Subset of CodedEvent table related to ethnicity"],
["ICD10Dictionary","Translation for ICD10 (diagnosis) codes"],
["ICNARC","Intensive treatment unit (ITU) admissions and ventilation"],
["LastestBuildTime",""],
["MedicationDictionary","Dictionary of medicines in primary care"],
["MedicationIssue","Issue date for medicines in primary care"],
["MedicationIssue_CVD_Meds","Subset of MedicationIssue table for CVD (cardiovascular disease)"],
["MedicationList_CVD_Meds","List of medicines for CVD (cardiovascular disease)"],
["MedicationRepeat","Repeat medications issued in primary care"],
["MedicationSensitivity", "Drug allergies recorded in primary care"],
["ONS_Deaths","All cause deaths from ONS (includes non-hospital deaths)"],
["Organisation","Practice codes"],
["Patient","DOB, DOD, sex as recorded in primary care"],
["PatientAddress","Patient address type, location, ruralness, deprivation score"],
["QOFClusterReference","QOF codess with links to CTV3 codes and descriptions"],
["RegistrationHistory","Patient's practice registration history"],
["SGSS_Negative","Negative lab results for covid test"],
["SGSS_Positive","Negative lab results for covid test"],
["sysdiagrams",""],
["UnitDictionary","Ranges and units for CTV3 codes with units"],
["Vaccination","Patient vaccination record"],
["VaccinationReference","Vaccination info"]
], columns=["name", "Description"])

df1 = df.merge(desc, on="name", how="left")

# +
# increase column display limit so that we can see all columns for each dataframe
pd.set_option('display.max_columns', 200)

for i, table in enumerate(df1['name']):
    sql = f"select TOP 10  * from {table}"
    display(Markdown(f"## {table}"))
    desc = df1.iloc[i]["Description"]
    display (desc)
    display(pd.read_sql(sql, cnxn).head())
