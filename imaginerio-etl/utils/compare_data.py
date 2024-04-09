import os

import pandas as pd

current_file = "input/jstor.xls"
current = pd.read_excel(current_file)

new_file = os.path.join("jstor_download", os.listdir("jstor_download")[0])
new = pd.read_excel(new_file)

comparison = new.compare(current, keep_shape=True)
changes = comparison.notna().any(axis=1)
to_process = new[changes]

os.replace(new_file, current_file)
to_process.to_excel("data/input/items_to_process.xls", engine="openpyxl")
