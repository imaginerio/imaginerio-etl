import dagster as dg
import pandas as pd


@dg.solid(input_defs=[dg.InputDefinition("camera", root_manager_key="camera_root")])
def geolocalization(context, camera):
    features = camera.features
    l1, l2, l3 = [], [], []

    for feature in features:
        l1.append(feature["properties"]["Source ID"])
        l2.append(feature["properties"]["latitude"])
        l3.append(feature["properties"]["longitude"])

    dic = {
        "identifier": l1,
        "latitude": l2,
        "longitude": l3,
    }

    df_camera = pd.DataFrame.from_dict(dic)
    return df_camera


@dg.solid(input_defs=[dg.InputDefinition("jstor", root_manager_key="jstor_root")])
def rename_columns(context, jstor):

    jstor = jstor.rename(
        columns={
            "SSID": "identifier",
            "Title[19462]": "Title",
            "Creator[19460]": "Creator",
            "Description in English[19481]": "description:en",
            "Media URL": "media",
            "fabrication method": "fabrication_method",
            "First Display Year[19466]": "First Year",
            "Last Display Year[19467]": "Last Year",
            "source URL": "source_url",
            # "width": "image_width",
            # "height": "image_height",
            # "depicts": "wikidata_depicts",
            "item set": "tem_set",
        }
    )

    return jstor


@dg.solid(output_defs=[dg.OutputDefinition(io_manager_key="pandas_csv", name="jstor")])
def select_columns(context, jstor, df_camera):
    df = pd.merge(jstor, df_camera, on=["identifier"], how="left")

    df_jstor = df[
        [
            "identifier",
            "Title",
            "Creator",
            "description:en",
            "description:pt",
            ""Type",
            "materials",
            "fabrication_method",
            "Date",
            "First Year",
            "Last Year",
            "source_url",
            "rights",
            "license",
            "attribution",
            "image_width",
            "image_height",
            "wikidata_depicts",
            "item_sets",
            "media",
            "altitude",
            "longitude",
        ]
    ]

    return df_jstor
