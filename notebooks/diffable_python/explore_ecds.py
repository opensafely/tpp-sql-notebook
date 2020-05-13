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
import os

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
EC_Conclusion_Date,
EC_Decision_To_Admit_Date as EC_admit_date
from ECDS e
'''

ecds = pd.read_sql(sql, cnxn)
ecds["ecds_flag"] = 1
ecds["admit_flag"] = np.where(pd.notnull(ecds["EC_admit_date"]), 1, 0)
ecds["EC_Conclusion_Date"] = pd.to_datetime(ecds["EC_Conclusion_Date"])
ecds["EC_admit_date"] = pd.to_datetime(ecds["EC_admit_date"])

# make a copy of table grouped by patient
ecds2 = ecds.copy().groupby("Patient_ID").max().reset_index()
ecds2 = ecds2.rename(columns={"EC_Conclusion_Date":"EC_Conclusion_Date_latest","EC_admit_date":"EC_admit_date_latest"})

display(Markdown(f"**ECDS Summary**"))
print("No of patients: ", ecds2["Patient_ID"].nunique())
print("No of patients admitted: ", ecds2["admit_flag"].sum())
print("Latest date: ", ecds2["EC_Conclusion_Date_latest"].max())
# -

# # Add linkable data if using dummy data

# +
dummy = pd.DataFrame([[10007,'2020-03-25','2020-03-25',1,1]], columns=['Patient_ID', 'EC_Conclusion_Date','EC_admit_date','admit_flag','ecds_flag'])

if password == 'ahsjdkaJAMSHDA123[':
    ecds = ecds.append(dummy)
    ecds["EC_Conclusion_Date"] = pd.to_datetime(ecds["EC_Conclusion_Date"])
    ecds["EC_admit_date"] = pd.to_datetime(ecds["EC_admit_date"])

ecds.head()
# -

# ### ECDS diagnosis

# +
sql = '''-- ecds diagnoses:
select Patient_ID,
DiagnosisCode,
CASE WHEN LEFT(DiagnosisCode,15) = '124075100000010' THEN 1 ELSE 0 END AS diagnosis_flag,
count(*) as diagnosis_code_count
from ECDS_EC_Diagnoses
GROUP BY patient_ID, DiagnosisCode
'''

d = pd.read_sql(sql, cnxn)
d["diagnosis_flag"] = 1

display(Markdown(f"**ECDS Diagnosis Summary**"))
print("No of patients: ", d["Patient_ID"].nunique())

# -

# # Link to other datasets

# ### Positive in SGSS

# +
sql = '''-- SGSS positives:
select Patient_ID, max(Lab_Report_Date) as pos_Lab_Report_Date_latest, count(*) AS positives
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
icu["IcuAdmissionDateTime_latest"] = pd.to_datetime(icu["IcuAdmissionDateTime_latest"])

display(Markdown(f"**ICNARC (ICU) Summary**"))
print("No of patients: ", icu["Patient_ID"].nunique())
print("Latest date: ", icu["IcuAdmissionDateTime_latest"].max())


# -

# # Compare latest dates found in each dataset

# +
def days_ago(DT=datetime(2020,3,3)):
    return (date.today()-datetime.date(DT)).days

print ("ECDS max date: ", ecds2["EC_Conclusion_Date_latest"].max(), ",  ", days_ago(ecds2["EC_Conclusion_Date_latest"].max()), " days ago")
print ("SGSS max date: ", p["pos_Lab_Report_Date_latest"].max(), ",  ", days_ago(p["pos_Lab_Report_Date_latest"].max()), " days ago")
print ("ICU max admit date: ", icu["IcuAdmissionDateTime_latest"].max(), ",  ", days_ago(icu["IcuAdmissionDateTime_latest"].max()), " days ago")
# -

# # What are common diagnoses?

# **Basic frequency of diagnosis codes (note not restricted on date of attendance)**

# +
d2 = d.groupby("DiagnosisCode")[["diagnosis_code_count"]].sum()

# suppress low values
d2.loc[d2["diagnosis_code_count"]<=5, "diagnosis_code_count"] = "1-5"
# show top 20 diagnoses
d2.sort_values(by="diagnosis_code_count", ascending=False).head(20)
# -

# ## Patients with positive test results near time of attendance

# +
ep = ecds.merge(p, on="Patient_ID", how="inner")

# restrict to matched testing/A&E dates
# attending A&E within 30d of positive test / positive test within 7 days of attending A&E
ep = ep.loc[(ep["EC_Conclusion_Date"] - ep["pos_Lab_Report_Date_latest"] < "30 days") & (ep["pos_Lab_Report_Date_latest"] - ep["EC_Conclusion_Date"] < "7 days")]

# remove remaining duplicates (people with multiple positive tests and/or attendances within the limits above)
ep = ep.groupby("Patient_ID").max().reset_index().drop("positives", axis=1)

# summarise data by week
ep = ep.loc[ep["EC_Conclusion_Date"].dt.year == 2020]
ep["week"] = ep["EC_Conclusion_Date"].dt.week
ep2 = ep.groupby("week")[["ecds_flag","admit_flag"]].sum()

# suppress low values
ep2.loc[ep2["ecds_flag"]<=5, "ecds_flag"] = "1-5"
ep2.loc[ep2["admit_flag"]<=5, "admit_flag"] = "1-5"
ep2
# -

# # What diagnoses are recorded in A&E for patients testing positive?

# +
epd = ep.merge(d, on="Patient_ID", how="left")

epd2 = epd.groupby("DiagnosisCode")[["diagnosis_code_count"]].sum()

# suppress low values
epd2.loc[epd2["diagnosis_code_count"]<=5, "diagnosis_code_count"] = "1-5"

# show top 20 diagnoses
epd2.sort_values(by="diagnosis_code_count", ascending=False).head(20)
# -

# # What ethnicities and ages are found in A&E?

# +
sql = '''-- Ethnicity:
select Patient_ID, 
CTV3Code, 
ConsultationDate,
MAX(ConsultationDate) OVER (PARTITION BY Patient_ID) as latest_date
from EthnicityCodedEvent
WHERE CTV3Code NOT IN ('9SZ..','XaJRB','XE0oc','XaE4B','XactD','9S...','XaBEN','134O.')-- exclude unknown codes
group by Patient_ID, CTV3Code, ConsultationDate
'''
eth = pd.read_sql(sql, cnxn)
# take only the latest recorded ethnicity for each patient
eth = eth.loc[eth['ConsultationDate']==eth['latest_date']]
eth = eth.drop(['ConsultationDate','latest_date'], axis=1)

eth_groups = pd.read_csv(os.path.join('..','data','opensafely-ethnicity.csv'))
# descriptions:
eth_groups2 = pd.DataFrame([[1, "White"], [2, "Mixed"], [3, "Black"], [4, "Asian or Asian British"], [5, "Other"]], columns=["Grouping_6","ethnicity"])
eth_groups = eth_groups.merge(eth_groups2, on="Grouping_6").drop(["Grouping_16","Grouping_6"], axis=1)

# find patient ethnicity groups
eth2 = eth.merge(eth_groups[["Code", "ethnicity"]], left_on="CTV3Code", right_on="Code", how="left").drop(["Code","CTV3Code"], axis=1)
eth2["ethnicity"].fillna("Unknown", inplace=True)

# add useful dummy data
dummy = pd.DataFrame([[10007,"Asian or Asian British"]], columns=['Patient_ID', 'ethnicity'])
if password == 'ahsjdkaJAMSHDA123[':
    eth2 = eth2.append(dummy)

display(Markdown(f"**Population Age Group Summary**"))
eth2.groupby("ethnicity").count()

# +
from datetime import datetime

sql = '''-- DOB:
select Patient_ID, 
MIN(DateOfBirth) AS DateOfBirth -- use min in case of any duplicate entries
from Patient
GROUP BY Patient_ID
'''
age = pd.read_sql(sql, cnxn)
age["age"] = datetime.date(pd.to_datetime('2020-04-01')) - age["DateOfBirth"]
age["age"] = (age["age"] / np.timedelta64(1, "Y")).astype(int)

# assign age groups
conditions = [
    (age['age'] < 18 ),
    (age['age'] < 65 ),
    (age['age'] < 80 )]
choices = ['0_<18', '18_<65', '65_<80']
age['age_group'] = np.select(conditions, choices, default='80+')

age = age.drop(["DateOfBirth","age"], axis=1)

display(Markdown(f"**Population Age Group Summary**"))
age.groupby("age_group").count()

# +
epae = ep.merge(age, on="Patient_ID", how="left")
epae = epae.merge(eth2, on="Patient_ID", how="left")

display(Markdown(f"**Summary of Ages and Ethnicities in A&E population with positive test result**"))
epae.groupby(["age_group","ethnicity"])[["Patient_ID"]].nunique().unstack()

# -

# # ECDS merged with SGSS and ICU data

# +
def merge_data(ecds2, p, n, d, icu):
    e2 = ecds2.merge(p, on="Patient_ID", how="outer")
    e2 = e2.merge(n, on="Patient_ID", how="outer")
    e2 = e2.merge(d, on="Patient_ID", how="left")
    e2 = e2.merge(icu, on="Patient_ID", how="outer")

    # replace nulls:
    # first for date columns
    cols = ["EC_Conclusion_Date_latest","pos_Lab_Report_Date_latest","neg_Lab_Report_Date_latest", "IcuAdmissionDateTime_latest", "EC_admit_date_latest"]
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
    ecds_patient_count = e2["ecds_flag"].sum()
    sgss_patient_count = e2["sgss_flag"].sum()
    icu_patient_count = e2["icu_flag"].sum()
    
    return e2, ecds_patient_count, sgss_patient_count, icu_patient_count

def ecds_percentage(x=1):
    return round(100*x/ecds_patient_count,0)

def sgss_percentage(x=1):
    return round(100*x/sgss_patient_count,0)

def icu_percentage(x=1):
    return round(100*x/icu_patient_count,0)

e2, ecds_patient_count, sgss_patient_count, icu_patient_count = merge_data(ecds2, p, n, d, icu)
e2.drop("Patient_ID", axis=1).sum()
# -

# # Weekly date cut-offs

# +
# create list of weekly dates to assess

latest_date = max(ecds2["EC_Conclusion_Date_latest"].max(), p["pos_Lab_Report_Date_latest"].max(), icu["IcuAdmissionDateTime_latest"].max())

l = []
for i in range(4):
     l.append( [-i-1, latest_date + pd.Timedelta(days=-7*i-7)] )

l = pd.DataFrame(l, columns=["week_no", "date"])
l


# +
   
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
    ecds1 = ecds2.loc[ecds2["EC_Conclusion_Date_latest"] <= w]
    p1 = p.loc[p["pos_Lab_Report_Date_latest"] <= w]
    n1 = n.loc[n["neg_Lab_Report_Date_latest"] <= w]
    icu1 = icu.loc[icu["IcuAdmissionDateTime_latest"] <= w]
    
    e2, ecds_patient_count, sgss_patient_count, icu_patient_count = merge_data(ecds1, p1, n1, d, icu1)
    
    r0 = results_table(e2, ecds_patient_count, sgss_patient_count, icu_patient_count, w)
    r = r.merge(r0, suffixes=["","_"], left_index=True, right_index=True)
    
display (r)

