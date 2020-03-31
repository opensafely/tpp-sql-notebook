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

from lib.data_gathering import search_terms_to_df

qof = pd.read_csv('../data/QoFClusteres_CTV3Codes - Sheet1.csv')

qof.head()

htn_list = ['HYP', 'HTMAX', 'HYP2', 'HYPEXC', 'HYPINVITE', 'HYPPCADEC', 'HYPPCAPU']

htn_qof = search_terms_to_df(htn_list, col_name='ClusterId', df_name=qof)

htn_qof.head()

htn_qof.to_csv('../data/htn.csv')


