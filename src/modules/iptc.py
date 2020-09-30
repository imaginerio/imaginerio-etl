import os
from iptcinfo3 import IPTCInfo
import pandas as pd

JPEG_HD = os.environ["JPEG_HD"]

df = pd.read_csv(os.environ["IMPORT_OMEKA"])
df.set_index("dcterms:identifier", inplace=True)


files = [os.path.join(JPEG_HD, file) for file in os.listdir(JPEG_HD)]


for i, file in enumerate(files):
    if file.endswith(".jpg"):
        basename = os.path.split(file)[1]
        name = basename.split(".")[0]
        print(name)
        info = IPTCInfo(file, force=True, inp_charset="UTF-8")
        # info["reference number"] = df.loc[name, "dcterms:identifier"]
        info["credit"] = df.loc[name, "dcterms:creator"]
        info["object name"] = df.loc[name, "dcterms:title"]
        info["caption/abstract"] = df.loc[name, "dcterms:description"]
        info["image type"] = df.loc[name, "dcterms:type"]
        # info["image orientation"] = f"{df.loc[name, "schema:width"]}cm x {df.loc[name, "schema:height"]}cm"
        info["keywords"] = list(df.loc[name, "foaf:depicts"].split("||"))
        info["release date"] = df.loc[name, "dcterms:date"]
        info["release time"] = df.loc[name, "dcterms:temporal"]
        info["country/primary location name"] = "Brazil"
        info["province/state"] = "RJ"
        info["city"] = "Rio de Janeiro"
        info["custom1"] = str(df.loc[name, "latitude"]) + "S"
        info["custom2"] = str(df.loc[name, "longitude"]) + "W"
        info["source"] = "Instituto Moreira Salles/IMS"
        info["copyright notice"] = "This image is in the Public Domain"
        info.save_as(basename)
        print(f"Tagged image {i+1} of {len(files)}")
    else:
        continue

