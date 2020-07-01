from pathlib import Path
from datetime import datetime
import shutil

import pandas as pd
from bokeh.plotting import output_file, show
from bokeh.layouts import column, layout
from tqdm import tqdm

from modules import report, maps


def dashboard(METADATA_PATH, PBAR):
    """
    Generates an HTML file with dashboard and map using bokeh
    """

    try:

        PBAR.set_description("Updating Report")
        # hbar = report.update_hbar(METADATA_PATH)
        # pie = report.update_pie(METADATA_PATH)
        PBAR.update(5)

        # load dashboard
        dashboard_plot = report.update(METADATA_PATH)

        PBAR.set_description("Updating Map")
        map_plot = maps.update(METADATA_PATH)
        PBAR.update(25)

        # export
        output_file("./index.html", title="Situated Views")
        show(
            layout(
                [[dashboard_plot["hbar"], dashboard_plot["pie"]], [map_plot]],
                sizing_mode="stretch_both",
            )
        )

    except Exception as e:
        print(str(e))


# dashboard("./metadata/metadata.csv")


def omeka_csv(METADATA_PATH):

    # read final dataframe
    omeka_df = pd.read_csv(METADATA_PATH)

    # datetime to year strings
    omeka_df["date"] = omeka_df["date"].dt.strftime("%Y")
    omeka_df["start_date"] = omeka_df["start_date"].dt.strftime("%Y")
    omeka_df["end_date"] = omeka_df["end_date"].dt.strftime("%Y")

    # join years into interval
    omeka_df["interval"] = omeka_df["start_date"] + "/" + omeka_df["end_date"]
    omeka_df = omeka_df.drop(columns=["start_date", "end_date"])

    # save csv
    omeka_df.to_csv("omeka-import.csv", index=False)

    # print dataframe
    print(omeka_df.head())


def img_to_commons(METADATA_PATH, IMAGES_PATH):
    # Get unplubished geolocated images
    final_df = pd.read_csv(METADATA_PATH)
    commons_df = pd.DataFrame(
        final_df[
            final_df["geometry"].notna()
            & final_df["img_hd"].notna()
            & final_df["wikidata_image"].isna()
        ]
    )

    # Create folder with images to be sent
    today = datetime.now()

    new_folder = IMAGES_PATH + "commons_" + today.strftime("%Y%m%d")

    Path(new_folder).mkdir(parents=True, exist_ok=True)

    for id in commons_df["id"]:
        shutil.copy2(f"./images/jpeg-hd/{id}.jpg", new_folder)
