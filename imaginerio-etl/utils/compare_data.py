import os

import pandas as pd

current_file = "data/input/jstor.xls"
current_data = pd.read_excel(current_file)

new_file = os.path.join("jstor_download", os.listdir("jstor_download")[0])
new_data = pd.read_excel(new_file)

comparison = new_data.compare(current_data, keep_shape=True)
changes = comparison.notna().any(axis=1)
to_process = new_data[changes]

os.replace(new_file, current_file)

if not to_process.loc[to_process["Status[30205]"] == "in imagineRio"].empty:
    to_process.to_excel("data/input/items_to_process.xls", engine="openpyxl")
