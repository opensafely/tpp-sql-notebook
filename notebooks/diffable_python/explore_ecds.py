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

# ### Inspect columns

# +
# increase column display limit so that we can see all rows in summary data
sql = '''-- main ecds table
select TOP 1 *
from ECDS
'''

ecds = pd.read_sql(sql, cnxn)
pd.set_option('display.max_rows', 200)

pd.Series(ecds.columns)
# -

# ### Main ECDS table

# +
sql = '''-- main ecds table
select Patient_ID, 
max(EC_Conclusion_Date) as EC_Conclusion_Date_latest
from ECDS e
group by Patient_ID
'''

ecds = pd.read_sql(sql, cnxn)
ecds["ecds_flag"] = 1
ecds["EC_Conclusion_Date_latest"] = pd.to_datetime(ecds["EC_Conclusion_Date_latest"])

display(Markdown(f"**ECDS Summary**"))
print("No of patients: ", ecds["Patient_ID"].nunique())
print("Latest date: ", ecds["EC_Conclusion_Date_latest"].max())
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

# +
sql = '''-- SGSS positives:
select DISTINCT Patient_ID, max(Lab_Report_Date) as pos_Lab_Report_Date_latest, count(*) AS positives
from SGSS_Positive
group by Patient_ID
'''
p = pd.read_sql(sql, cnxn)
p["positive_flag"] = 1
p["pos_Lab_Report_Date_latest"] = pd.to_datetime(p["pos_Lab_Report_Date_latest"])

display(Markdown(f"**SGSS Positive Summary**"))
print("No of patients: ", p["Patient_ID"].nunique())
print("Latest lab result date: ", p["pos_Lab_Report_Date_latest"].max())
# -

# ### Negative in SGSS

# +
sql = '''
-- SGSS negatives:
select Patient_ID, max(Lab_Report_Date) as neg_Lab_Report_Date_latest, count(*) AS negatives
from SGSS_Negative
group by Patient_ID
'''

n = pd.read_sql(sql, cnxn)
n["negative_flag"] = 1
n["neg_Lab_Report_Date_latest"] = pd.to_datetime(n["neg_Lab_Report_Date_latest"])

display(Markdown(f"**SGSS Negative Summary**"))
print("No of patients: ", n["Patient_ID"].nunique())
print("Latest lab result date: ", n["neg_Lab_Report_Date_latest"].max())
# -

# ## ICU

# +
sql = '''-- icnarc ICU admissions
select Patient_ID, 
max(IcuAdmissionDateTime) as IcuAdmissionDateTime_latest
from ICNARC
group by Patient_ID
'''

icu = pd.read_sql(sql, cnxn)
icu["icu_flag"] = 1
icu["IcuAdmissionDateTime_latest"] = pd.to_datetime(icu["IcuAdmissionDateTime_latest"], unit='D', format="%d/%m/%Y")

display(Markdown(f"**ICNARC (ICU) Summary**"))
print("No of patients: ", icu["Patient_ID"].nunique())
print("Latest date: ", icu["IcuAdmissionDateTime_latest"].max())
# -

# ## How many patients in ECDS have a positive or negative result in SGSS

# **first check how well periods overlap:**

# +
from datetime import date
from datetime import datetime

def days_ago(DT=datetime(2020,3,3)):
    return (date.today()-datetime.date(DT)).days

print ("ECDS max date: ", ecds["EC_Conclusion_Date_latest"].max(), ",  ", days_ago(ecds["EC_Conclusion_Date_latest"].max()), " days ago")
print ("SGSS max date: ", p["pos_Lab_Report_Date_latest"].max(), ",  ", days_ago(p["pos_Lab_Report_Date_latest"].max()), " days ago")
print ("ICU max admit date: ", icu["IcuAdmissionDateTime_latest"].max(), ",  ", days_ago(icu["IcuAdmissionDateTime_latest"].max()), " days ago")

# +
import numpy as np

e2 = ecds.merge(p, on="Patient_ID", how="outer")
e2 = e2.merge(n, on="Patient_ID", how="outer")
e2 = e2.merge(d, on="Patient_ID", how="left")
e2 = e2.merge(icu, on="Patient_ID", how="outer")

#replace nulls
cols = ["EC_Conclusion_Date_latest","pos_Lab_Report_Date_latest","neg_Lab_Report_Date_latest", "IcuAdmissionDateTime_latest"]
for c in cols:
    e2[c].fillna(datetime(1900,1,1), inplace=True)
    
e2.fillna(0, inplace=True)

# create additional flags
e2["sgss_flag"] = np.where(e2["positive_flag"]+e2["negative_flag"]>0, 1, 0)
e2["pos_neg_flag"] = np.where((e2["positive_flag"]>0)&(e2["negative_flag"]>0), 1, 0)
e2["ecds_no_sgss_flag"] = np.where( (e2["ecds_flag"]==1)&(e2["sgss_flag"]==0), 1, 0)
e2["sgss_no_ecds_flag"] = np.where( (e2["ecds_flag"]==0)&(e2["sgss_flag"]==1), 1, 0)
e2["icu_no_ecds_flag"] = np.where( (e2["ecds_flag"]==0)&(e2["icu_flag"]==1), 1, 0)
e2["icu_no_sgss_flag"] = np.where( (e2["sgss_flag"]==0)&(e2["icu_flag"]==1), 1, 0)
e2["ecds_and_positive_flag"] = np.where(e2["positive_flag"]+e2["ecds_flag"]==2, 1, 0)
e2["diagnosis_and_positive_flag"] = np.where(e2["positive_flag"]+e2["diagnosis_flag"]==2, 1, 0)

# patient counts and percentage calculations
ecds_patient_count = e2["ecds_flag"].sum()
sgss_patient_count = e2["sgss_flag"].sum()
icu_patient_count = e2["icu_flag"].sum()

def ecds_percentage(x=1):
    return round(100*x/ecds_patient_count,0)

def sgss_percentage(x=1):
    return round(100*x/sgss_patient_count,0)

def icu_percentage(x=1):
    return round(100*x/icu_patient_count,0)


# print summary information
    
display(Markdown(f"**ECDS Summary**"))
print("No of patients in ECDS: ", ecds_patient_count)
print("Patients with covid diagnosis recorded:", e2["diagnosis_flag"].sum(), "(", ecds_percentage(e2["diagnosis_flag"].sum()), "% ) \n")
print("Latest date: ", e2["EC_Conclusion_Date_latest"].max(),  ", " , days_ago(e2["EC_Conclusion_Date_latest"].max()), "days ago \n")

display(Markdown(f"**ICNARC (ICU) Summary**"))
print("No of patients in ICNARC: ", icu_patient_count)
print("Latest date: ", e2["IcuAdmissionDateTime_latest"].max(),  ", " , days_ago(e2["IcuAdmissionDateTime_latest"].max()), "days ago \n")


display(Markdown(f"**SGSS Summary**"))
print("Total patients in SGSS: ", sgss_patient_count)
print("Latest date: ", e2["pos_Lab_Report_Date_latest"].max(),  ", " , days_ago(e2["pos_Lab_Report_Date_latest"].max()), "days ago \n")

print("Total positive results: ", e2["positives"].sum())
print("No of patients with at least one positive result: ", e2["positive_flag"].sum(), 
      "(", sgss_percentage(e2["positive_flag"].sum()), "% )")

print("Total negative results: ", e2["negatives"].sum())
print("No of patients with at least one negative result: ", e2["negative_flag"].sum(), 
      "(", sgss_percentage(e2["negative_flag"].sum()), "% )")

print("No of patients with at least one positive AND negative result: ", e2["pos_neg_flag"].sum(), 
      "(", sgss_percentage(e2["pos_neg_flag"].sum()), "% )\n")



display(Markdown(f"**ECDS/SGSS/ICU Summary**"))

print("ECDS patients with positive lab results:", e2["ecds_and_positive_flag"].sum(),
      "(", ecds_percentage(e2["ecds_and_positive_flag"].sum()), "% )")
      
print("ECDS patients with any covid diagnosis and positive lab result:", 
      e2["diagnosis_and_positive_flag"].sum(), "(", ecds_percentage(e2["diagnosis_and_positive_flag"].sum()), "% )")

print("ECDS patients with no lab results:", e2["ecds_no_sgss_flag"].sum(),
     "(", ecds_percentage(e2["ecds_no_sgss_flag"].sum()), "% )\n")

print("No of patients in SGSS not in A&E (ECDS): ", e2["sgss_no_ecds_flag"].sum(), 
      "(", sgss_percentage(e2["sgss_no_ecds_flag"].sum()), "% )\n")

print("No of patients in ICU not in A&E (ECDS): ", e2["icu_no_ecds_flag"].sum(), 
      "(", icu_percentage(e2["icu_no_ecds_flag"].sum()), "% )")

print("No of patients in ICU not in SGSS: ", e2["icu_no_sgss_flag"].sum(), 
      "(", icu_percentage(e2["icu_no_sgss_flag"].sum()), "% )\n")

## note : need to add date restrictions
