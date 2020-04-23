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

# ### Main ECDS table

# +
sql = '''-- main ecds table
select Patient_ID, 
max(EC_Conclusion_Date) as EC_Conclusion_Date_latest
from ECDS e
group by Patient_ID
'''

ecds = pd.read_sql(sql, cnxn)
display(Markdown(f"**ECDS Summary**"))
print("No of patients: ", ecds["Patient_ID"].nunique())
print("Latest date: ", ecds["EC_Conclusion_Date_latest"].max())
# -

# ### Inspect columns

# +
# increase column display limit so that we can see all rows in summary data
pd.set_option('display.max_rows', 200)

ecds.max()
# -

# ### ECDS diagnosis

# +
sql = '''-- ecds diagnoses:
select distinct Patient_ID
from ECDS_EC_Diagnoses
where LEFT(DiagnosisCode,15) = '124075100000010'
'''

d = pd.read_sql(sql, cnxn)
d["diagnosis_flag"] = 1
display(Markdown(f"**ECDS Diagnosis Summary**"))
print("No of patients: ", d["Patient_ID"].nunique())

## empty table
# -

# ### Positive in SGSS

sql = '''-- SGSS positives:
select DISTINCT Patient_ID, max(Lab_Report_Date) as pos_Lab_Report_Date_latest, count(*) AS positives
from SGSS_Positive
group by Patient_ID
'''
p = pd.read_sql(sql, cnxn)
display(Markdown(f"**SGSS Positive Summary**"))
print("No of patients: ", p["Patient_ID"].nunique())
print("Latest lab result date: ", p["pos_Lab_Report_Date_latest"].max())

# ### Negative in SGSS

# +
sql = '''
-- SGSS negatives:
select Patient_ID, max(Lab_Report_Date) as neg_Lab_Report_Date_latest, count(*) AS negatives
from SGSS_Negative
group by Patient_ID
'''

n = pd.read_sql(sql, cnxn)
display(Markdown(f"**SGSS Negative Summary**"))
print("No of patients: ", n["Patient_ID"].nunique())
print("Latest lab result date: ", n["neg_Lab_Report_Date_latest"].max())
# -

# ## How many patients in ECDS have a positive or negative result in SGSS

# **first check how well periods overlap:**

print ("ECDS max date: ", ecds["EC_Conclusion_Date_latest"].max())
print ("SGSS max date: ", p["pos_Lab_Report_Date_latest"].max())

# +
import numpy as np

e2 = ecds.merge(p, on="Patient_ID", how="left").fillna(0)
e2 = e2.merge(n, on="Patient_ID", how="left").fillna(0)
e2 = e2.merge(d, on="Patient_ID", how="left").fillna(0)


e2["positive_flag"] = np.where(e2["positives"]>0,1,0)
e2["negative_flag"] = np.where(e2["negatives"]>0,1,0)
e2["no_sgss_flag"] = np.where(e2["positive_flag"]+e2["negative_flag"]==0,1,0)
e2["diagnosis_and_positive_flag"] = np.where(e2["positive_flag"]+e2["diagnosis_flag"]==2,1,0)

patient_count = e2["Patient_ID"].nunique()

def percentage(x=1):
    return 100*x/patient_count
    
display(Markdown(f"**ECDS / SGSS Summary**"))
print("No of patients in ECDS: ", e2["Patient_ID"].nunique())
print("Latest date: ", e2["EC_Conclusion_Date_latest"].max(), "\n")

print("ECDS patients with any covid diagnosis:", e2["diagnosis_flag"].sum(), "(", percentage(e2["diagnosis_flag"].sum()), "%) \n")

print("ECDS patients with any covid diagnosis and positive lab result:", 
      e2["diagnosis_and_positive_flag"].sum(), "(", percentage(e2["diagnosis_and_positive_flag"].sum()), "%) \n")


print("ECDS patients with positive lab results:", e2["positive_flag"].sum(),
      "(", percentage(e2["positive_flag"].sum()), "%)")

print("ECDS patients with no lab results:", e2["no_sgss_flag"].sum(),
     "(", percentage(e2["no_sgss_flag"].sum()), "%)")
