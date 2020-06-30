import os

import pandas as pd
from tqdm import tqdm

from modules import camera, catalog, images, wikidata, portals, export


def main():
    """
    Execute all functions
    """

    CAMERA_PATH = "./metadata/camera/"  # reads camera.csv and camera.geojson
    CUMULUS_PATH = "./metadata/catalog/" + "cumulus.xml"
    PORTALS_PATH = "./metadata/catalog/" + "portals.csv"
    IMAGES_PATH = "./images/images.csv"
    WIKIDATA_PATH = "./metadata/wikidata/" + "wikidata.csv"
    # RIGHTS_PATH = "./metadata/rights/" + "rights.csv"
    METADATA_PATH = "./metadata/metadata.csv"

    try:

        with tqdm(total=100) as pbar:

            pbar.set_description("Loading Image Paths")
            images_df = images.load(IMAGES_PATH)
            pbar.update(5)

            pbar.set_description("Loading Camera Positions")
            camera_df = camera.load(CAMERA_PATH)
            pbar.update(5)

            pbar.set_description("Loading Cumulus Metadata")
            catalog_df = catalog.load(CUMULUS_PATH)
            pbar.update(10)

            pbar.set_description("Checking Cumulus Portals")
            portals_df = portals.load(PORTALS_PATH)
            pbar.update(15)

            pbar.set_description("Checking Wikidata")
            wikidata_df = wikidata.load(WIKIDATA_PATH)
            pbar.update(15)

            pbar.set_description("Updating Metadata File")
            dataframes = [portals_df, images_df, camera_df, wikidata_df]
            final_df = catalog_df
            for dataframe in dataframes:
                final_df = pd.merge(
                    final_df, dataframe, on=["id"], how="left", validate="one_to_one",
                )
                pbar.update(5)
            final_df.to_csv(METADATA_PATH, index=False)

            pbar.set_description("Updating Dashboard")
            export.dashboard(METADATA_PATH, PBAR=pbar)
            pbar.update(25) 

            pbar.set_description("Done")
            pbar.close()

    except Exception as e:

        print(str(e))


if __name__ == "__main__":
    main()
