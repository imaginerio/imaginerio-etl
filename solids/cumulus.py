import dagster as dg
import dagster_pandas as dp
from dagster_pandas.data_frame import DataFrame
from numpy.core.numeric import True_
import pandas as pd
from pandas._libs.tslibs import NaT
from tests.dataframe_types import *
from tests.objects_types import *


# solids cumulus
@dg.solid(
    input_defs=[dg.InputDefinition("root", root_manager_key="cumulus_root")],
    output_defs=[dg.OutputDefinition(dagster_type=dp.DataFrame)],
)
def xml_to_df(context, root):
    """
    Build Pandas DataFrame from XML file
    """
    # Find the uids

    uids = {}
    for thing in root[0][0]:
        uids[thing.attrib["uid"]] = thing[0].text

    table = {}
    for field in uids.values():
        table[field] = []

    outDict = {"table": table, "uids": uids}

    # Fill the records
    ns = {"cumulus": "http://www.canto.com/ns/Export/1.0"}
    for thing in root[1]:
        added = set()
        for field_value in thing.findall("cumulus:FieldValue", ns):
            try:
                if len(field_value) == 0:
                    value = field_value.text.strip()
                else:
                    value = field_value[0].text.strip().split(":")
                    value = str(value).strip("[']")

                outDict["table"][outDict["uids"][field_value.attrib["uid"]]].append(
                    value
                )
                added.add(field_value.attrib["uid"])
            except KeyError:
                continue
        for missing in outDict["uids"].keys() - added:
            try:
                outDict["table"][outDict["uids"][missing]].append(None)
            except KeyError:
                continue
    formated_table = outDict["table"]
    cumulus_df = pd.DataFrame(formated_table)

    # load
    cumulus_df = cumulus_df.astype(
        {"DATA": str, "DATA LIMITE INFERIOR": str, "DATA LIMITE SUPERIOR": str},
        copy=False,
    )
    cumulus_df[["DATA LIMITE SUPERIOR", "DATA LIMITE INFERIOR"]] = cumulus_df[
        ["DATA LIMITE SUPERIOR", "DATA LIMITE INFERIOR"]
    ].applymap(lambda x: x.split(".")[0])

    return cumulus_df


@dg.solid(output_defs=[dg.OutputDefinition(dagster_type=dp.DataFrame)])
def organize_columns(context, df: dp.DataFrame):
    """
    Rename columns, remove file extension from identifiers,
    normalize creators names and drop duplicates
    """
    # rename columns
    cumulus_df = df.rename(
        columns={
            "Record Name": "Source ID",
            "CÓDIGO DE IDENTIFICAÇÃO PRELIMINAR": "preliminary id",
            "TÍTULO": "Title",
            "RESUMO": "Description (Portuguese)",
            "AUTORIA": "Creator",
            "DATA": "Date",
            "DATA LIMITE INFERIOR": "First Year",
            "DATA LIMITE SUPERIOR": "Last Year",
            "DIMENSÃO": "dimensions",
            "PROCESSO FORMADOR DA IMAGEM": "Fabrication Method",
            "DESIGNAÇÃO GENÉRICA": "Materials",
            "FORMATO PADRÃO": "format",
        },
    )
    # select columns
    cumulus_df = cumulus_df[
        [
            "Source ID",
            "Title",
            "Description (Portuguese)",
            "Creator",
            "Date",
            "First Year",
            "Last Year",
            "Materials",
            "Fabrication Method",
            "format",
            "dimensions",
            "preliminary id",
        ]
    ]

    # remove file extension
    cumulus_df["Source ID"] = cumulus_df["Source ID"].str.split(".", n=1, expand=True)[
        0
    ]

    # remove duplicates
    cumulus_df = cumulus_df.drop_duplicates(subset="Source ID", keep="last")

    # reverse cretor name
    cumulus_df["Creator"] = cumulus_df["Creator"].str.replace(
        r"(.+),\s+(.+)", r"\2 \1")

    return cumulus_df


@dg.solid(
    output_defs=[
        dg.OutputDefinition(
            io_manager_key="pandas_csv", name="creators", dagster_type=dp.DataFrame
        )
    ]
)  # save list of Creatorss for rights assessment
def creators_list(context, df: dp.DataFrame):
    """
    Create list of creators for right assessment
    """
    creators_df = df["Creator"].unique()
    listed_creators = pd.DataFrame(creators_df)
    listed_creators.set_index(0, inplace=True)

    listed_creators.name = "creators"

    return listed_creators


# extract dimensions
@dg.solid(output_defs=[dg.OutputDefinition(dagster_type=dp.DataFrame)])
def extract_dimensions(context, df: dp.DataFrame):
    """
    Infer width and height in mm from dimensions field
    """
    dimensions = df["dimensions"].str.extract(
        r"[.:] (?P<height>\d+,?\d?) [Xx] (?P<width>\d+,?\d?)"
    )

    df["Width (mm)"] = dimensions["width"].str.replace(
        ",", ".").astype(float) * 10
    df["Height (mm)"] = dimensions["height"].str.replace(
        ",", ".").astype(float) * 10

    return df


@dg.solid(output_defs=[dg.OutputDefinition(dagster_type=dp.DataFrame)])
def format_date(context, df: dp.DataFrame):
    """
    Infer circa dates and format date string according to accuracy
    """

    # fill dates
    df.loc[df["Date"].str.count(r"[-\/^a-z]") == 0, "date_accuracy"] = "year"
    df.loc[df["Date"].str.count(r"[\/-]") == 1, "date_accuracy"] = "month"
    df.loc[df["Date"].str.count(r"[\/-]") == 2, "date_accuracy"] = "day"
    df.loc[df["Date"].str.contains(
        r"[a-z]", na=False), "date_accuracy"] = "circa"

    # format date
    df["First Year"] = df["First Year"].str.extract(r"([\d\/-]*\d{4}[-\/\d]*)")
    df["Last Year"] = df["Last Year"].str.extract(r"([\d\/-]*\d{4}[-\/\d]*)")

    df["datetime"] = df["Date"].str.extract(r"([\d\/-]*\d{4}[-\/\d]*)")
    df[["First Year", "Last Year", "datetime"]] = df[
        ["First Year", "Last Year", "datetime"]
    ].astype("str")

    df[["First Year", "Last Year", "datetime"]] = df[
        ["First Year", "Last Year", "datetime"]
    ].applymap(lambda x: pd.to_datetime(x, errors="coerce", yearfirst=True))

    circa = df["date_accuracy"] == "circa"

    firstna = df["First Year"].isna()
    lastna = df["Last Year"].isna()

    df.loc[circa & df["First Year"].isna(), "First Year"] = df[
        "datetime"
    ] - pd.DateOffset(years=5)
    df.loc[circa & df["Last Year"].isna(), "Last Year"] = df[
        "datetime"
    ] + pd.DateOffset(years=5)

    df.loc[df["First Year"].isna(), "First Year"] = df["datetime"]
    df.loc[df["Last Year"].isna(), "Last Year"] = df["datetime"]

    # datetime to string according to date accuracy
    df["First Year"] = df["First Year"].dt.strftime("%Y")
    df["Last Year"] = df["Last Year"].dt.strftime("%Y")

    df.loc[df["Date"].str.contains(r"[a-z]", na=False) & df["Date"].notna(), "Date"] = (
        df["datetime"].dt.strftime("%Y") + " circa"
    )  # circa
    df.loc[df["Date"].str.fullmatch(r"\d{4}") & df["Date"].notna(), "Date"] = df[
        "datetime"
    ].dt.strftime(
        "%Y"
    )  # year
    df.loc[df["Date"].str.fullmatch(r"\d+[\/-]\d+") & df["Date"].notna(), "Date"] = df[
        "datetime"
    ].dt.strftime(
        "%m/%Y"
    )  # month
    df.loc[
        df["Date"].str.fullmatch(
            r"\d+[\/-]\d+[\/-]\d+") & df["Date"].notna(), "Date"
    ] = df["datetime"].dt.strftime("%d/%m/%Y")

    cumulus = df
    cumulus.name = "cumulus"

    return cumulus


@dg.solid(output_defs=[dg.OutputDefinition(dagster_type=dp.DataFrame)])
def create_columns(context, df_cumulus: main_dataframe_types):
    """
    Map processes and formats according to Wikidata entities
    """
    df_cumulus.loc[
        df_cumulus["Materials"] == "FOTOGRAFIA/ Papel", "Materials"
    ] = "Photographic print"

    df_cumulus.loc[
        df_cumulus["Materials"] == "REPRODUÇÃO FOTOMECÂNICA/ Papel", "Materials"
    ] = "Photomechanical print"

    df_cumulus.loc[
        df_cumulus["Materials"] == "NEGATIVO/ Vidro", "Materials"
    ] = "Glass plate negative"

    df_cumulus.loc[
        df_cumulus["Materials"] == "DIAPOSITIVO/ Vidro", "Materials"
    ] = "Glass diapositive"

    df_cumulus.loc[df_cumulus["format"] == "Estereoscopia", "Materials"] = (
        df_cumulus["Materials"] + "||Stereoscopy"
    )

    df_cumulus.loc[
        df_cumulus["Fabrication Method"] == "AUTOCHROME / Corante e prata",
        "Fabrication Method",
    ] = "Autochrome"

    df_cumulus.loc[
        df_cumulus["Fabrication Method"] == "ALBUMINA/ Prata", "Fabrication Method"
    ] = "Albumine"

    df_cumulus.loc[
        df_cumulus["Fabrication Method"] == "GELATINA/ Prata", "Fabrication Method"
    ] = "Silver gelatin"

    df_cumulus.loc[
        df_cumulus["Fabrication Method"] == "COLÓDIO/ Prata", "Fabrication Method"
    ] = "Collodion"

    df_cumulus.loc[
        df_cumulus["Fabrication Method"] == "LANTERN SLIDE / Prata",
        "Fabrication Method",
    ] = "Lantern slide"

    df_cumulus.loc[
        df_cumulus["Fabrication Method"] == "AMBROTIPIA/ Prata",
        "Fabrication Method",
    ] = "Ambrotype"

    df_cumulus.loc[
        df_cumulus["Fabrication Method"] == "COLOTIPIA/ Pigmento",
        "Fabrication Method",
    ] = "Collotype print"

    df_cumulus.loc[
        df_cumulus["Fabrication Method"] == "FOTOGRAVURA/ Pigmento",
        "Fabrication Method",
    ] = "Photogravure"

    df_cumulus.loc[
        df_cumulus["Fabrication Method"] == "MEIO-TOM/ Pigmento",
        "Fabrication Method",
    ] = "Photogravure"

    df_cumulus["Description (English)"] = "-"
    df_cumulus["Type"] = "Photograph"
    df_cumulus["Item Set"] = "All||Views"
    df_cumulus["Source"] = "Instituto Moreira Salles"
    df_cumulus["License"] = "-"
    df_cumulus["Rights"] = "-"
    df_cumulus["Attribution"] = ""
    df_cumulus["Smapshot ID"] = ""

    return df_cumulus


@dg.solid(
    output_defs=[
        dg.OutputDefinition(
            io_manager_key="pandas_csv", name="cumulus", dagster_type=dp.DataFrame
        )
    ]
)
def select_columns(context, df_cumulus: main_dataframe_types):
    """
    Filter columns for saving CSV file
    """
    df = df_cumulus[
        [
            "Source ID",
            "Title",
            "Creator",
            "Description (English)",
            "Description (Portuguese)",
            "Date",
            "datetime",
            "date_accuracy",
            "First Year",
            "Last Year",
            "Type",
            "Item Set",
            "Source",
            "Materials",
            "Fabrication Method",
            "Rights",
            "License",
            "Attribution",
            "Width (mm)",
            "Height (mm)",
            "Smapshot ID",
            "preliminary id",
        ]
    ]

    return df.set_index("Source ID")
