import os

import pandas as pd
from tqdm import tqdm

import camera, catalog, export, images, maps, omeka, portals, report, wikidata


def update_metadata(catalog, dataframes):
    final_df = catalog
    review_df = pd.DataFrame(columns=["id", "Not in"])
    for dataframe in dataframes:
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
        # filter and label items that are on catalog only
        missing = missing[missing["Not in"] == "left_only"]
        missing["Not in"] = f"{dataframe.name}"
        # add those items to review_df
        review_df = review_df.append(missing[["id", "Not in"]], ignore_index=True)
    review_df = review_df.groupby("id", as_index=False).agg(list)
    review_df.to_csv(os.environ["REVIEW"], index=False)
    final_df.to_csv(os.environ["METADATA"], index=False)
    return None


def main():
    """
    Execute all functions
    """

    try:

        with tqdm(total=100) as pbar:

            # Available images
            pbar.set_description("Loading Image Paths")
            images_df = images.load(os.environ["IMAGES"])
            images_df.name = "Images"
            pbar.update(5)

            # Coordinates and polygons
            pbar.set_description("Loading Camera Positions")
            camera_df = camera.load(
                os.environ["CAMERA_CSV"], os.environ["CAMERA_GEOJSON"]
            )
            camera_df.name = "Camera"
            pbar.update(5)

            # Items metadata
            pbar.set_description("Loading Cumulus Metadata")
            catalog_df = catalog.load(os.environ["CUMULUS_XML"])
            catalog_df.name = "Catalog"
            pbar.update(5)

            # List items on Portals
            pbar.set_description("Checking Cumulus Portals")
            portals_df = portals.load(os.environ["PORTALS"])
            portals_df.name = "Portals"
            pbar.update(20)

            # List items on Wikidata
            pbar.set_description("Checking Wikidata")
            wikidata_df = wikidata.load(os.environ["WIKIDATA"])
            wikidata_df.name = "Wikidata"
            pbar.update(10)

            # List items on Omeka
            pbar.set_description("Checking Omeka")
            omeka_df = omeka.load(os.environ["OMEKA_API"])
            omeka_df.name = "Omeka"
            pbar.update(20)

            # Merge all dataframes
            pbar.set_description("Updating Metadata Files")
            dataframes = [images_df, camera_df, portals_df, wikidata_df, omeka_df]
            update_metadata(catalog_df, dataframes)
            pbar.update(10)

            # Create dashboard
            pbar.set_description("Generating import files and dashboard...")
            export.load(os.environ["METADATA"])
            pbar.update(25)

            pbar.set_description("Done")
            pbar.close()

    except Exception as e:

        print(str(e))


if __name__ == "__main__":
    main()
