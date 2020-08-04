import os

import pandas as pd
from tqdm import tqdm

from modules import camera, catalog, images, wikidata, portals, export, omeka


def update_metadata(catalog, dataframes):
    final_df = catalog
    review_df = pd.DataFrame(columns=["id", "_merge"])
    for dataframe in dataframes:
        final_df = catalog
        final_df = pd.merge(
            final_df, dataframe, on=["id"], how="left", validate="one_to_one",
        )
        missing = pd.merge(
            catalog,
            dataframe,
            on="id",
            how="outer",
            indicator="Not in",
            validate="one_to_one",
            copy=True,
        )
        missing = missing[missing["Not in"] == "left_only"]
        missing["Not in"] = f"{dataframe.name}"
        review_df = review_df.append(missing[["id", "Not in"]], ignore_index=True)
    review_df = review_df.groupby("id", as_index=False).agg(list)
    review_df.to_csv(os.environ["REVIEW_PATH"], index=False)
    final_df.to_csv(os.environ["METADATA_PATH"], index=False)
    return final_df, review_df


def main():
    """
    Execute all functions
    """

    try:

        with tqdm(total=100) as pbar:

            pbar.set_description("Loading Image Paths")
            images_df = images.load(os.environ["IMAGES_PATH"])
            images_df.name = "Images"
            pbar.update(10)

            pbar.set_description("Loading Camera Positions")
            camera_df = camera.load(os.environ["CAMERA_PATH"])
            camera_df.name = "Camera"
            pbar.update(10)

            pbar.set_description("Loading Cumulus Metadata")
            catalog_df = catalog.load(os.environ["CUMULUS_XML"])
            catalog_df.name = "Catalog"
            pbar.update(10)

            pbar.set_description("Checking Cumulus Portals")
            portals_df = portals.load(os.environ["PORTALS_PATH"])
            portals_df.name = "Portals"
            pbar.update(10)

            pbar.set_description("Checking Wikidata")
            wikidata_df = wikidata.load(os.environ["WIKIDATA_PATH"])
            wikidata_df.name = "Wikidata"
            pbar.update(10)

            pbar.set_description("Checking Omeka")
            omeka_df = omeka.load(os.environ["OMEKA_API_URL"])
            omeka_df.name = "Omeka"
            pbar.update(10)

            pbar.set_description("Updating Metadata Files")
            dataframes = [images_df, camera_df, portals_df, wikidata_df, omeka_df]
            update_metadata(catalog_df, dataframes)
            pbar.update(10)

            pbar.set_description("Exporting omeka.csv")
            export.omeka_csv(os.environ["METADATA_PATH"])
            pbar.update(10)

            pbar.set_description("Exporting gis.csv")
            export.gis_csv(os.environ["METADATA_PATH"])
            pbar.update(10)

            pbar.set_description("Updating Dashboard")
            export.dashboard(os.environ["METADATA_PATH"], PBAR=pbar)
            pbar.update(10)

            pbar.set_description("Done")
            pbar.close()

    except Exception as e:

        print(str(e))


if __name__ == "__main__":
    main()
