import dagster as dg
import pandas as pd


# solids cumulus
@dg.solid(
    input_defs=[dg.InputDefinition("root", root_manager_key="cumulus_root")],
)
def xml_to_df(context, root):
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


@dg.solid
def organize_columns(context, df):
    # rename columns
    cumulus_df = df.rename(
        columns={
            "Record Name": "Source ID",
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
        ]
    ]

    # remove file extension
    cumulus_df["SSID"] = cumulus_df["SSID"].str.split(".", n=1, expand=True)

    # remove duplicates
    cumulus_df = cumulus_df.drop_duplicates(subset="SSID", keep="last")

    # reverse cretor name
    cumulus_df["Creator"] = cumulus_df["Creator"].str.replace(
        r"(.+),\s+(.+)", r"\2 \1"
    )

    return cumulus_df


@dg.solid(
    output_defs=[dg.OutputDefinition(io_manager_key="pandas_csv", name="Creatorss")]
)  # save list of Creatorss for rights assessment
def creators_list(context, df):
    creators_df = df["Creator"].unique()
    listed_creators = pd.DataFrame(creators_df)
    listed_creators.set_index(0, inplace=True)

    listed_creators.name = "Creatorss"

    return listed_creators


@dg.solid  # extract dimensions
def extract_dimensions(context, df):
    dimensions = df["dimensions"].str.extract(
        r"[.:] (?P<height>\d+,?\d?) [Xx] (?P<width>\d+,?\d?)"
    )
    df["width"] = dimensions["width"]
    df["height"] = dimensions["height"]

    return df


@dg.solid
def dates_accuracy(context, df):

    # format date
    df["date_copy"] = df["Date"]
    df["Date"] = df["Date"].str.extract(r"([\d\/-]*\d{4}[-\/\d]*)")

    df["First Year"] = df["First Year"].str.extract(r"([\d\/-]*\d{4}[-\/\d]*)")
    df["Last Year"] = df["Last Year"].str.extract(r"([\d\/-]*\d{4}[-\/\d]*)")

    circa = df["date_copy"].str.contains(r"[a-z]", na=False)
    year = df["date_copy"].str.count(r"[\/-]") == 0
    month = df["date_copy"].str.count(r"[\/-]") == 1
    day = df["date_copy"].str.count(r"[\/-]") == 2

    df[["Date", "First Year", "Last Year"]] = df[
        ["Date", "First Year", "Last Year"]
    ].astype("str")

    df[["Date", "First Year", "Last Year"]] = df[
        ["Date", "First Year", "Last Year"]
    ].applymap(lambda x: pd.to_datetime(x, errors="coerce", yearfirst=True))

    # fill dates
    startna = df["First Year"].isna()
    endna = df["Last Year"].isna()

    df.loc[circa & startna, "First Year"] = df["Date"] - pd.DateOffset(years=5)
    df.loc[circa & endna, "Last Year"] = df["Date"] + pd.DateOffset(years=5)
    df.loc[startna, "First Year"] = df["Date"]
    df.loc[endna, "Last Year"] = df["Date"]

    # datetime to string according to date accuracy

    df["First Year"] = df["First Year"].dt.strftime("%Y")
    df["Last Year"] = df["Last Year"].dt.strftime("%Y")
    df.loc[circa, "Date"] = df["Date"].dt.strftime("%Y") + "circa"
    df.loc[year, "Date"] = df["Date"].dt.strftime("%Y")
    df.loc[month, "Date"] = df["Date"].dt.strftime("%m-%Y")
    df.loc[day, "Date"] = df["Date"].dt.strftime("%d-%m-%Y")

    cumulus = df
    cumulus.name = "cumulus"

    return cumulus.set_index("SSID")


@dg.solid
def create_columns(context, df_cumulus):

    # omeka_df.loc[
    #     omeka_df["Materials"] == "FOTOGRAFIA/ Papel",
    #     ("dcterms:type:pt", "dcterms:type:en"),
    # ] = (
    #     "wikidata.org/wiki/Q56055236 Fotografia em papel",
    #     "wikidata.org/wiki/Q56055236 Photographic print",
    # )
    # omeka_df.loc[
    #     omeka_df["Materials"] == "REPRODUÇÃO FOTOMECÂNICA/ Papel",
    #     ("dcterms:type:pt", "dcterms:type:en"),
    # ] = (
    #     "wikidata.org/wiki/Q100575647 Impressão fotomecânica",
    #     "wikidata.org/wiki/Q100575647 Photomechanical print",
    # )
    # omeka_df.loc[
    #     omeka_df["Materials"] == "NEGATIVO/ Vidro",
    #     ("dcterms:type:pt", "dcterms:type:en"),
    # ] = (
    #     "wikidata.org/wiki/Q85621807 Negativo de vidro",
    #     "wikidata.org/wiki/Q85621807 Glass plate negative",
    # )


    df_cumulus.loc[
        df_cumulus["Materials"] == "DIAPOSITIVO/ Vidro",
        "Materials",
    ] = "Glass diapositive"

    df_cumulus.loc[df_cumulus["format"] == "Estereoscopia", "Materials"] = df_cumulus["Materials"] + "||Stereoscopy"

    df_cumulus.loc[
        df_cumulus["Fabrication Method"] == "AUTOCHROME / Corante e prata",
        "Fabrication Method",
    ] = "Autochrome"

    df_cumulus.loc[
        df_cumulus["Fabrication Method"] == "ALBUMINA/ Prata",
        "Fabrication Method",
    ] = "Albumine"

    df_cumulus.loc[
        df_cumulus["Fabrication Method"] == "GELATINA/ Prata",
        "Fabrication Method"
    ] = "Silver gelatin"
    )
    df_cumulus.loc[
        df_cumulus["Fabrication Method"] == "COLÓDIO/ Prata",
        "Fabrication Method",
    ] = "Collodion"

    df_cumulus.loc[
        df_cumulus["Fabrication Method"] == "LANTERN SLIDE / Prata",
        "Fabrication Method",
    ] = "Lantern slide"



    df_cumulus["Description (English)"] = ""
    df_cumulus["Type (Artstor Classification)"] = "Photographs"
    df_cumulus["Item Set (text field)"] = "all||views"
    df_cumulus["Source"] = "Instituto Moreira Salles"
    df_cumulus["License"] = ""
    df_cumulus["Rights"] = ""
    df_cumulus["License"] = ""
    df_cumulus["Attribution"] = ""
    df_cumulus["Smapshot ID"] = ""

    return df_cumulus


@dg.solid(output_defs=[dg.OutputDefinition(io_manager_key="pandas_csv", name="metadata")])
def select_columns(context, df_cumulus):

    df = df_cumulus[
        [
            "Source ID",
            "Title",
            "Creator",
            "Description (English)",
            "Description (Portuguese)",
            "Date",
            "First Year",
            "Last Year",
            "Type",
            "Item Set (text field)",
            "Source",
            "Materials",
            "Fabrication Method",
            "Rights",
            "License",
            "Attribution",
            "Width (mm)",
            "Height (mm)",
            "Item Set",
            "Smapshot ID",
            "format---",
        ]
    ]

    return df_cumulus
