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
import numpy as np
from datetime import date
from datetime import datetime

server = 'covid.ebmdatalab.net,1433'
database = 'OPENCoronaExport' 
username = 'SA'
password = 'ahsjdkaJAMSHDA123[' 
cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()

# ## Total patients included 

# +
sql = '''-- all patients in primary care:
select DISTINCT Patient_ID
from Patient
'''
tpp = pd.read_sql(sql, cnxn)

display(Markdown(f"**TPP Summary**"))
print("No of registered patients: ", tpp["Patient_ID"].nunique())

# -

# ## Positive & negative tests (SGSS)

# +
sql = '''-- SGSS positives:
select Patient_ID, max(Lab_Report_Date) as pos_Lab_Report_Date_latest, count(*) AS positives
from SGSS_Positive
group by Patient_ID
'''
p = pd.read_sql(sql, cnxn)
p["positive_flag"] = 1
p["pos_Lab_Report_Date_latest"] = pd.to_datetime(p["pos_Lab_Report_Date_latest"])

sql = '''
-- SGSS negatives:
select Patient_ID, max(Lab_Report_Date) as neg_Lab_Report_Date_latest, count(*) AS negatives
from SGSS_Negative
group by Patient_ID
'''

n = pd.read_sql(sql, cnxn)
n["negative_flag"] = 1
n["neg_Lab_Report_Date_latest"] = pd.to_datetime(n["neg_Lab_Report_Date_latest"])

sgss = p.merge(n, on="Patient_ID", how="outer")


display(Markdown(f"**SGSS Summary**"))
print("Latest lab result date: ", sgss["pos_Lab_Report_Date_latest"].max())

print("Total tests: ", int(sgss["positives"].sum()+sgss["negatives"].sum()))
print("No of patients tested: ", sgss["Patient_ID"].nunique())

print("No of patients with positives: ", int(sgss["positive_flag"].sum()))
print("No of patients with negatives: ", int(sgss["negative_flag"].sum()))
# -

# ## ECDS (A&E)

# +
sql = '''-- main ecds table
select Patient_ID, 
max(EC_Conclusion_Date) as EC_Conclusion_Date_latest,
max(EC_Decision_To_Admit_Date) as EC_admit_date_latest
from ECDS e
group by Patient_ID
'''

ecds = pd.read_sql(sql, cnxn)
ecds["ecds_flag"] = 1
ecds["admit_flag"] = np.where(pd.notnull(ecds["EC_admit_date_latest"]), 1, 0)
ecds["EC_Conclusion_Date_latest"] = pd.to_datetime(ecds["EC_Conclusion_Date_latest"])

# diagnoses
sql = '''-- ecds diagnoses:
select distinct Patient_ID
from ECDS_EC_Diagnoses
where LEFT(DiagnosisCode,15) = '124075100000010'
'''

d = pd.read_sql(sql, cnxn)
d["diagnosis_flag"] = 1


ecds = ecds.merge(d, on="Patient_ID", how="outer")

ecds["patients_with_diagnosis_no_ae"] = np.where((ecds["diagnosis_flag"] == 1)&(ecds["ecds_flag"] == 0), 1, 0)
ecds["patients_with_covid_diagnosis"] = np.where((ecds["diagnosis_flag"] == 1)&(ecds["ecds_flag"] == 1), 1, 0)

display(Markdown(f"**ECDS (A&E) Summary**"))
print("No of patients: ", ecds["Patient_ID"].nunique())
print("Of which have covid diagnosis: ", ecds["patients_with_covid_diagnosis"].sum())
print("No of patients with covid diagnosis but no attendance record: ",ecds["patients_with_diagnosis_no_ae"].sum())
print("No of patients admitted: ", ecds["admit_flag"].sum())
print("Latest date: ", ecds["EC_Conclusion_Date_latest"].max())
# -

# ## ICU
# Need to add ICU deaths information when available

# +
sql = '''-- icnarc ICU admissions
select Patient_ID, 
max(Ventilator) as ventilator,
max(IcuAdmissionDateTime) as IcuAdmissionDateTime_latest
from ICNARC
group by Patient_ID
'''

icu = pd.read_sql(sql, cnxn)
icu["icu_flag"] = 1
icu["IcuAdmissionDateTime_latest"] = pd.to_datetime(icu["IcuAdmissionDateTime_latest"], unit='D', format="%d/%m/%Y")

display(Markdown(f"**ICNARC (ICU) Summary**"))
print("No of patients: ", icu["Patient_ID"].nunique())
print("Number ventilated: ", int(icu["ventilator"].sum()))
print("Latest date: ", icu["IcuAdmissionDateTime_latest"].max())
# -

# ## CPNS (deaths in hospital)

# +
sql = '''-- CPNS deaths
select Patient_ID, 
LocationOfDeath, -- e.g. ICU
DateOfDeath
from CPNS
'''

cpns = pd.read_sql(sql, cnxn)
cpns["cpns_flag"] = 1
cpns["DateOfDeath"] = pd.to_datetime(cpns["DateOfDeath"])

display(Markdown(f"**CPNS (deaths) Summary**"))
print("No of patients: ", cpns["Patient_ID"].count(), "(", cpns["Patient_ID"].nunique(), "unique)")
print("Latest date: ", cpns["DateOfDeath"].max())
# -

# ### Summarise deaths by location

cpns.groupby(["LocationOfDeath"])[["Patient_ID"]].count()

# ## ONS Deaths

# +
sql = '''-- ONS deaths
select Patient_ID, 
dod
from ONS_Deaths
'''

ons = pd.read_sql(sql, cnxn)
ons["ons_flag"] = 1
ons["dod"] = pd.to_datetime(ons["dod"])

display(Markdown(f"**ONS (deaths) Summary**"))
print("No of patients: ", ons["Patient_ID"].count(), "(", ons["Patient_ID"].nunique(), "unique)")
print("Latest date: ", ons["dod"].max())
# -

# ### Weekly date cut-offs

# +
# create list of weekly dates to assess

latest_date = max(ecds["EC_Conclusion_Date_latest"].max(), p["pos_Lab_Report_Date_latest"].max(), icu["IcuAdmissionDateTime_latest"].max())

l = []
for i in range(7):
     l.append( [-i-1, latest_date + pd.Timedelta(days=-7*i-7)] )

l = pd.DataFrame(l, columns=["week_no", "date"])
l


# -

# # Summary

# +
def merge_data(sgss, ecds, icu, cpns, ons):
    e2 = sgss.merge(ecds, on="Patient_ID", how="outer")
    e2 = e2.merge(icu, on="Patient_ID", how="outer")
    e2 = e2.merge(cpns, on="Patient_ID", how="outer")
    e2 = e2.merge(ons, on="Patient_ID", how="outer")
    
    # replace nulls:
    # first for date columns
    cols = ["EC_Conclusion_Date_latest","pos_Lab_Report_Date_latest","neg_Lab_Report_Date_latest", "IcuAdmissionDateTime_latest", "EC_admit_date_latest", "DateOfDeath", "dod"]
    for c in cols:
        e2[c].fillna(datetime(1900,1,1), inplace=True)
    # other columns
    e2.fillna(0, inplace=True)

    # create additional flags
    e2["sgss_flag"] = np.where(e2["positive_flag"]+e2["negative_flag"]>0, 1, 0)
    e2["pos_neg_flag"] = np.where((e2["positive_flag"]>0)&(e2["negative_flag"]>0), 1, 0)
    e2["ecds_no_sgss_flag"] = np.where( (e2["ecds_flag"]==1)&(e2["sgss_flag"]==0), 1, 0)
    e2["sgss_no_ecds_flag"] = np.where( (e2["ecds_flag"]==0)&(e2["sgss_flag"]==1), 1, 0)
    e2["icu_no_ecds_flag"] = np.where( (e2["ecds_flag"]==0)&(e2["icu_flag"]==1), 1, 0)
    e2["icu_no_sgss_flag"] = np.where( (e2["sgss_flag"]==0)&(e2["icu_flag"]==1), 1, 0)
    e2["ecds_and_positive_flag"] = np.where(e2["positive_flag"]+e2["ecds_flag"]==2, 1, 0)
    e2["ecds_diagnosed_and_admitted_flag"] = np.where((e2["ecds_flag"]==1)&(e2["diagnosis_flag"]==1)&(e2["admit_flag"]==1), 1, 0)
    e2["diagnosis_and_positive_flag"] = np.where(e2["positive_flag"]+e2["diagnosis_flag"]==2, 1, 0)

    # patient counts and percentage calculations
    sgss_patient_count = e2["sgss_flag"].sum()
    ecds_patient_count = e2["ecds_flag"].sum()
    icu_patient_count = e2["icu_flag"].sum()
    
    return e2, ecds_patient_count, sgss_patient_count, icu_patient_count

e2, sgss_patient_count, ecds_patient_count, icu_patient_count = merge_data(sgss, ecds, icu, cpns, ons)

def ecds_percentage(x=1):
    return round(100*x/ecds_patient_count,0)

def sgss_percentage(x=1):
    return round(100*x/sgss_patient_count,0)

def icu_percentage(x=1):
    return round(100*x/icu_patient_count,0)

e2.drop("Patient_ID", axis=1).sum()


# +
def days_ago(DT=datetime(2020,3,3)):
    return (date.today()-datetime.date(DT)).days

def ecds_percentage2(X):
    return str(X)+" ("+str(ecds_percentage(X))+"%)"
def sgss_percentage2(X):
    return str(X)+" ("+str(sgss_percentage(X))+"%)"
def icu_percentage2(X):
    return str(X)+" ("+str(icu_percentage(X))+"%)"

def bracs_d(X):
    return str(X)+" ("+str(days_ago(X))+ " days ago)"



def results_table(e2, ecds_patient_count, sgss_patient_count, icu_patient_count, w):
    results = pd.DataFrame() 
    results = results.append([["ECDS","Total patients",ecds_patient_count],
    ["ECDS", "Patients admitted", e2["admit_flag"].sum()],
    ["ECDS", "Patients diagnosed", ecds_percentage2(e2["diagnosis_flag"].sum())],
    ["ECDS", "Patients diagnosed and admitted", ecds_percentage2(e2["ecds_diagnosed_and_admitted_flag"].sum())],                         
    ["ECDS", "Latest date", bracs_d(e2["EC_Conclusion_Date_latest"].max())],

    ["ICNARC (ICU)", "Total patients", icu_patient_count], 
    ["ICNARC (ICU)", "Latest date", bracs_d(e2["IcuAdmissionDateTime_latest"].max())],

    ["SGSS", "Total patients", sgss_patient_count],
    ["SGSS", "Latest date", bracs_d(e2["pos_Lab_Report_Date_latest"].max())],
    ["SGSS", "Total positive results", e2["positives"].sum()],
    ["SGSS", "Patients with at least one positive", sgss_percentage2(e2["positive_flag"].sum())],
    ["SGSS", "Total negative results", e2["negatives"].sum()],
    ["SGSS", "Patients with at least one negative", sgss_percentage2(e2["negative_flag"].sum())],
    ["SGSS", "Patients with at least one positive AND negative", sgss_percentage2(e2["pos_neg_flag"].sum())],                   

    ["Combined", "ECDS patients with positive lab results", ecds_percentage2(e2["ecds_and_positive_flag"].sum())],
    ["Combined", "ECDS patients with any covid diagnosis and positive lab result", ecds_percentage2(e2["diagnosis_and_positive_flag"].sum())],
    ["Combined", "ECDS patients with no lab results", ecds_percentage2(e2["ecds_no_sgss_flag"].sum())],
    ["Combined","Patients in SGSS not in A&E (ECDS)", sgss_percentage2(e2["sgss_no_ecds_flag"].sum())],
    ["Combined","Patients in ICU not in A&E (ECDS)", icu_percentage2(e2["icu_no_ecds_flag"].sum())],
    ["Combined","Patients in ICU not in SGSS", icu_percentage2(e2["icu_no_sgss_flag"].sum())]
    ])   
    results = results.rename(columns={0:"dataset", 1:"item", 2:str(w)}).set_index(["dataset","item"]) 
    return results

# latest data summary
r = results_table(e2, ecds_patient_count, sgss_patient_count, icu_patient_count, w=latest_date)

# add previous weeks' summaries
for i,w in enumerate(l["date"]): 
    ecds1 = ecds.loc[ecds["EC_Conclusion_Date_latest"] <= w]
    sgss1 = sgss.loc[sgss["pos_Lab_Report_Date_latest"] <= w]
    icu1 = icu.loc[icu["IcuAdmissionDateTime_latest"] <= w]
    cpns1 = cpns.loc[cpns["DateOfDeath"] <= w]
    ons1 = ons.loc[ons["dod"]<= w]
    
    e2, ecds_patient_count, sgss_patient_count, icu_patient_count = merge_data(sgss1, ecds1, icu1, cpns1, ons1)
    
    r0 = results_table(e2, ecds_patient_count, sgss_patient_count, icu_patient_count, w)
    r = r.merge(r0, suffixes=["","_"], left_index=True, right_index=True)
    
display (r)

