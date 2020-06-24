# ims catalog metadata from cumulus
import pandas as pd
import numpy as np
from xml.etree import ElementTree
from pprint import pprint




def xml_to_df(cumulus_xml):

    with open(xml) as f:
        tree = ElementTree.parse(f)
    root = tree.getroot()

    ns = {'cumulus': 'http://www.canto.com/ns/Export/1.0'}
    
    # Find the uids
    uids = {}
    for thing in root[0][0]:
        uids[thing.attrib['uid']] = thing[0].text

    table = {}
    for field in uids.values():
        table[field] = []

    print("Building the pandas DataFrame")

    # Fill the records
    for thing in root[1]:
        added = set()
        for field_value in thing.findall('cumulus:FieldValue', ns):
            #pprint(field_value.text)
            try:
                if len(field_value) == 0:
                    value = field_value.text.strip()
                    #print(value)
                else:
                    value = field_value[0].text.strip().split(':')
                    value = str(value).strip("['']")
                    #print(value)
                    
                table[uids[field_value.attrib['uid']]].append(value)
                added.add(field_value.attrib['uid'])
            except KeyError:
                continue
        for missing in uids.keys() - added:
            try:
                table[uids[missing]].append(None)
            except KeyError:
                continue

    # Create the actual DataFrame
    cumulus_df = pd.DataFrame(table)

    cumulus_df.to_csv("./metadata/catalog/cumulus.csv")

    return cumulus_df
    
    


def find_dates(date):
    """
    Find dates between 1500 and 2022 and parse to datetime
    """
    result = date.str.extract(r"(1[5-9]\d{2}|20[0-2]{2})")
    result = pd.to_datetime(date, errors="coerce")
    return result


def load(path):
    try:
        catalog_df = pd.read_excel(
            path,
            dtype={
                "DATA": str,
                "DATA LIMITE INFERIOR": str,
                "DATA LIMITE SUPERIOR": str,
            },
        )

        # rename columns
        catalog_df = catalog_df.rename(
            columns={
                "Record Name": "id",
                "TÍTULO": "title",
                "RESUMO": "description",
                "AUTORIA": "creator",
                "DATA": "date",
                "DATA LIMITE INFERIOR": "start_date",
                "DATA LIMITE SUPERIOR": "end_date",
                "DIMENSÃO": "dimensions",
                "PROCESSO FORMADOR DA IMAGEM": "fabrication_method",
                "LOCAL": "place",
                "DESIGNAÇÃO GENÉRICA": "type",
            },
        )

        # select columns
        catalog_df = catalog_df[
            [
                "id",
                "title",
                "description",
                "creator",
                "date",
                "start_date",
                "end_date",
                "type",
                "fabrication_method",
                "dimensions",
                "place",
            ]
        ]

        # remove file extension
        catalog_df["id"] = catalog_df["id"].str.split(".", n=1, expand=True)

        # remove duplicates
        catalog_df = catalog_df.drop_duplicates(subset="id", keep="last")

        # check dates accuracy
        catalog_df["accurate_date"] = np.where(
            catalog_df.date.astype(str).str.contains(r"[a-z]"), False, True
        )
        catalog_df["accurate_date"] = np.where(
            catalog_df.date.isnull(), False, catalog_df["accurate_date"]
        )

        # dates to datetime
        catalog_df["date"] = find_dates(catalog_df["date"])
        catalog_df["start_date"] = find_dates(catalog_df["start_date"])
        catalog_df["end_date"] = find_dates(catalog_df["end_date"])

        # reverse cretor name
        catalog_df["creator"] = catalog_df["creator"].str.replace(
            r"(.+),\s+(.+)", r"\2 \1"
        )

        # save list of creators for rights assessment
        creators_df = catalog_df["creator"].unique()
        pd.DataFrame(creators_df).to_csv("./metadata/rights/creators.csv", index=False)

        # fill empty start/end dates
        catalog_df.loc[
            (catalog_df["accurate_date"] == False) & (catalog_df["start_date"].isna()),
            "start_date",
        ] = catalog_df["date"] - pd.DateOffset(years=5)

        catalog_df.loc[
            (catalog_df["accurate_date"] == False) & (catalog_df["end_date"].isna()),
            "end_date",
        ] = catalog_df["date"] + pd.DateOffset(years=5)

        # extract dimensions
        dimensions_df = catalog_df["dimensions"].str.extract(
            r"[.:] (?P<height>\d+,?\d?) [Xx] (?P<width>\d+,?\d?)"
        )
        catalog_df["image_width"] = dimensions_df["width"]
        catalog_df["image_height"] = dimensions_df["height"]

        return catalog_df

    except Exception as e:

        print(str(e))
