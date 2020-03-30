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

import pandas as pd
import numpy as np

data = pd.read_csv('../data/analysis/study_pop.csv')

data.head()

data = data[['Patient_ID']]

data.head()

# +
outcome = [1, 0, 0,0,0,0]

data['death'] = np.random.randint(0, len(outcome), len(data))
data['death'] = data['death'].apply(lambda i: outcome[i])
# -

data.head()

data.to_csv('../data/analysis/outcome.csv')


