import os

import pandas as pd

from modules import camera, catalog, images, wikidata, portals, export


LINE = 80 * "-"


def main():
    """
    Execute all functions
    """

    CAMERA_PATH = "./metadata/camera/"  # reads camera.csv and camera.geojson
    CUMULUS_PATH = "./metadata/catalog/" + "cumulus.xlsx"
    PORTALS_PATH = "./metadata/catalog/" + "portals.csv"
    IMAGES_PATH = "./images/images.csv"
    WIKIDATA_PATH = "./metadata/wikidata/" + "wikidata.csv"
    # RIGHTS_PATH = "./metadata/rights/" + "rights.csv"
    METADATA_PATH = "./metadata/metadata.csv"

    try:
        images_df = images.load(IMAGES_PATH)
        print(LINE)

        camera_df = camera.load(CAMERA_PATH)
        print(LINE)

        catalog_df = catalog.load(CUMULUS_PATH)
        print(LINE)

        portals_df = portals.load(PORTALS_PATH)
        print(LINE)

        wikidata_df = wikidata.load(WIKIDATA_PATH)
        print(LINE)

        dataframes = [portals_df, images_df, camera_df, wikidata_df]

        final_df = catalog_df

        for dataframe in dataframes:
            final_df = pd.merge(
                final_df, dataframe, on=["id"], how="left", validate="one_to_one",
            )
            print(
                f"Total {len(dataframe)} Duplicates {len(dataframe[dataframe['id'].duplicated()])}"
            )

        print(final_df.head())

        # save merged dataset
        final_df.to_csv(METADATA_PATH, index=False)

        print(f"Dataset updated ({METADATA_PATH})")
        print(LINE)

        export.dashboard(METADATA_PATH)
        print(LINE)

    except Exception as e:

        print(str(e))


if __name__ == "__main__":
    print(LINE, "UPDATING METADATA", LINE)
    main()
