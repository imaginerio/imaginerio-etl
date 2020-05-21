import os

import pandas as pd

from modules import camera, catalog, images, maps  # rights, export, report

LINE = 80 * "-"

CAMERA_PATH = "./metadata/camera/"  # reads camera.csv and camera.geojson
CATALOG_PATH = "./metadata/catalog/" + "cumulus.xlsx"
IMAGES_PATH = "./images/images.csv"
# RIGHTS_PATH = "./metadata/rights/" + "rights.csv"
# WIKIDATA_PATH = "./metadata/wikidata/" + "wikidata.csv"
METADATA_PATH = "./metadata/metadata.csv"


def main():
    """
    Execute all functions
    """

    try:
        images_df = images.load(IMAGES_PATH)
        print(LINE)

        camera_df = camera.load(CAMERA_PATH)
        print(LINE)

        catalog_df = catalog.load(CATALOG_PATH)
        print(LINE)

        DF1 = pd.merge(
            catalog_df, camera_df, on="identifier", how="inner", validate="one_to_one"
        )
        final_df = pd.merge(
            DF1, images_df, on="identifier", how="inner", validate="one_to_one"
        )

        # save merged dataset
        final_df.to_csv(METADATA_PATH, index=False)

        print(f"Dataset updated ({METADATA_PATH})")
        print(LINE)

        # portals.load()
        # rights.load()
        # wikidata.load()
        # export.render()

        maps.update(METADATA_PATH)
        print(LINE)

        # report.render()

    except Exception as e:

        print(str(e))


if __name__ == "__main__":
    print(LINE, "UPDATING METADATA", LINE)
    main()
