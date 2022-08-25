import sys 
sys.path.insert(0, './classes')

import argparse
import os
import re

import numpy as np
import pandas as pd
from image import Highres, Image, Tif
from dotenv import load_dotenv
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

from helpers import logger, update_metadata

load_dotenv(override=True)


def get_images(metadata):
    """
    Walks directory tree and glob relevant files,
    instantiating Image objects for each
    """

    source = os.environ["SOURCE"]

    images = [
        Image(
            os.path.join(root, name),
            metadata,
        )
        for root, _, files in os.walk(source)
        for name in files
        if "FINALIZADAS" in root
        and name.endswith((".tif"))
        and not re.search("[av]\.tif$", name)
    ]

    logger.debug(f"Listed {len(images)} images to process")
    return images


def dispatch(image):
    """
    Copy failsafe TIFs, convert geolocated images
    to JPG, and separate files for review and backlog
    """

    if image.to_tif:
        image.copy_strategy(Tif())

    if image.to_jpg:
        image.copy_strategy(Highres())

    # if image.to_backlog or image.to_review:
    #     image.copy_strategy(Lowres())

    logger.debug(f"Dispatched image {image.id}")
    return image


def create_images_df(images):
    """
    Creates a dataframe with every image available and links to full size and thumbnail
    """

    prefix = os.environ["BUCKET"]

    id = [img.id for img in images]
    url = {"Media URL":[
        os.path.join(prefix, "iiif", img.id, "full", "max", "0", "default.jpg")
        # if img.is_geolocated
        if img.in_catalog
        else np.nan
        for img in images
    ]}
    images_df = pd.DataFrame(url, index=id)
    images_df.drop_duplicates(inplace=True)

    logger.debug(f"{len(images_df)} images available in hi-res")

    return images_df


def main():
    metadata = pd.read_csv(os.environ["IMS_METADATA"], index_col="Document ID")
    images = get_images(metadata)
    images_df = create_images_df(images)

    with logging_redirect_tqdm():
        for image in tqdm(images, desc="Handling images"):
            dispatch(image)
            # if image.is_geolocated:
            if image.in_catalog:
                #image.get_metadata(metadata)
                image.embed_metadata()
                # if not file_exists(image.id, "image"):
                #     upload_file_to_s3(
                #     os.path.join(os.environ["JPG"], image.jpg),
                #     target="iiif/{0}/full/max/0/default.jpg".format(image.id),
                #     mode=args.mode,
                # )
                # else:
                #     logger.debug(f"{image.id} already in bucket")
            else:
                continue

    update_metadata(images_df)
    #return images_df

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode", "-m", help="run mode", choices=["test", "prod"], default="test"
    )
    args = parser.parse_args()

    main()
    