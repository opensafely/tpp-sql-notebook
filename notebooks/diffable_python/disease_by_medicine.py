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

# * Factor: Respiratory issues
# * Threshold/Criteria: at least one prescription for 3 consecutive quarters in given medicines list
# * Rationale for inclusion: [UK Shielding Criteria](https://www.cas.mhra.gov.uk/ViewandAcknowledgment/ViewAlert.aspx?AlertID=103003)
# * Proposed Source of code list: the record
#
# Our definitely of repeat / regular is "a medicine has appeared at least once in three consecutive quarters".
#
# This is less conservative than the BSA approach, and we should document why this is.
#
# So for the "asthma defined as people on X inhalers and with Y clinical codes" question, then we would include someone who changes between listed inhalers (within list X) every month in our cohort
#

from lib import database 
from lib import codelists
import pandas as pd
import importlib
importlib.reload(database)  # for quick library development
importlib.reload(codelists) 
from IPython.display import display, Markdown

# +
# Some of the qof clusters that we might want to use
asthma_clinical_codes = "AST"
astham_ics_codes = "ASTTRTATRISK1"
asthma_drugs = "DRAST3" # Asthma-related drug treatment codes
copd_clinical_codes = "COPD" # Chronic obstructive pulmonary disease (COPD) codes
copd_clinical_codes_2 = "COPD2" # COPD codes. These are the same as COPD
copd_clinical_codes_3 = "DRCOPD1" # COPD diagnosis. Contains 4 codes not in COPD; and COPD contains 5 not in this
smoking_drugs = "DRSMOK11" # Asthma-related drug treatment codes
inhaled_therapy_drugs = "INDR" #C odes for inhaled therapy
# -


clinical_codes = codelists.get_codes_for_clusters([asthma_clinical_codes])
drug_codes = codelists.get_codes_for_clusters([asthma_drugs])

# However, we don't have CTV3 drug codes mapped to DMD ID yet, so we'll just use a bunch of random ones for now
df = database.sql_to_df("select FullName, DMD_ID from MedicationDictionary GROUP BY FullName, DMD_ID")
drug_codes = list(df[df.DMD_ID != ""].sample(100)["DMD_ID"].values)

importlib.reload(database) 
df3 = database.patients_with_codes_in_consecutive_quarters(
    clinical_code_list=list(clinical_codes), 
    medicine_code_list=list(drug_codes), 
    quarters=3)

df3
