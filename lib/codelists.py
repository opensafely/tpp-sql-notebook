import pandas as pd


qof_clusters = pd.read_csv("../data/QoFClusteres_CTV3Codes - Sheet1.csv")


def get_codes_for_clusters(cluster_ids):
    return qof_clusters.loc[
        qof_clusters["ClusterId"].isin(cluster_ids), "CTV3Code"
    ].values
