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

# In this notebook we are exploring vaccine reference tables priorit to constructing queries.

import pyodbc
import pandas as pd
from IPython.display import display, Markdown

server = 'covid.ebmdatalab.net,1433'
database = 'OPENCoronaExport' 
username = 'SA'
password = 'ahsjdkaJAMSHDA123[' 
cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()

# +
    sql = "select * from VaccinationReference"    
   
    vacc = (pd.read_sql(sql, cnxn))


# -
vacc.head(300)

#here we count number of unique entries
vacc.nunique()

## lets list the 48 different types of vaccine
vacc.VaccinationContent.unique()

# There are a few things here which aren't vaccines e.g. phytomenadione, palivizumab as well as Mikes content!

mikes = vacc[vacc['VaccinationContent'].str.contains("mike", case=False)]
mikes

fluvacc = vacc[vacc['VaccinationContent'].str.contains("influ", case=False)]
fluvacc

cnxn.close()
