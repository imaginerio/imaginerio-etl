import os

import pandas as pd

from metadata import camera, catalog  # rights, export, report


CAMERA_PATH = "./metadata/camera/"  # reads camera.csv and camera.geojson
CATALOG_PATH = "./metadata/catalog/" + "cumulus.xlsx"
# RIGHTS_PATH = "./metadata/rights/" + "rights.csv"
# WIKIDATA_PATH = "./metadata/wikidata/" + "wikidata.csv"


def main():
    """
    Execute all functions
    """

    try:
        # images.load() --> MARTIM

        camera_df = camera.load(CAMERA_PATH)
        catalog_df = catalog.load(CATALOG_PATH)
        # catalog_df.to_csv("catalog.csv", index=False)

        # rights.load()
        # export.render()
        # report.render()

    except Exception as e:

        print(str(e))


if __name__ == "__main__":
    print(80 * "-")
    print("UPDATING METADATA")
    print(80 * "-")

    main()

    print(80 * "-")

    # pandas saves all data regarding metadata in metadata/metadata.csv --> MAPA CINTIA
