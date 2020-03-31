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

# * Factor: Age
# * Threshold/Criteria: >= 70
# * Rationale for inclusion: [UK Shielding Criteria](https://www.cas.mhra.gov.uk/ViewandAcknowledgment/ViewAlert.aspx?AlertID=103003)
# * Proposed Source of code list: the record
#
#

from lib import database 
import importlib
importlib.reload(database)  # for quick library development
from IPython.display import display, Markdown

# +
database.patients_with_age_condition(">=", 70)


