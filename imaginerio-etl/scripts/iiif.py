import argparse
import json
import logging.config
import os
import random
import shutil
import sys
from math import *

import boto3
import pandas as pd
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

from ..config import *
from ..entities.item import Item
from ..utils.helpers import (
    get_collections,
    get_metadata,
    upload_folder_to_s3,
    upload_object_to_s3,
)
from ..utils.logger import CustomFormatter, logger

if __name__ == "__main__":
    logger.info("Parsing arguments")
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode", "-m", help="run mode", choices=["test", "prod"], default="test"
    )
    parser.add_argument("--index", "-i", help="index to run", default="all")
    args = parser.parse_args()

    metadata, vocabulary = get_metadata(JSTOR, VOCABULARY, args.index)
    collections = get_collections(metadata)
    manifests = []
    errors = []

    for index, (id, row) in enumerate(metadata.fillna("").iterrows()):
        logger.info(
            f"{CustomFormatter.LIGHT_BLUE}{index}/{len(metadata)} - Parsing item {id}"
        )
        item = Item(id, row, vocabulary)
        sizes = item.get_sizes() or item.tile_image()
        manifest = item.create_manifest(sizes)
        if manifest:
            manifests.append(manifest)
            upload_object_to_s3(manifest, f"iiif/{item._id}/manifest.json")
            # logger.info(
            #     f"{CustomFormatter.GREEN}Manifest {item._id} created and uploadedsuccesfully"
            # )
            for name in item.get_collections():
                collections[name].add_item_by_reference(manifest)
        else:
            logger.error(
                f"{CustomFormatter.RED}Couldn't create manifest for item {item._id}, skipping"
            )
            errors.append(item._id)

    for name in collections.keys():
        upload_object_to_s3(collections[name], f"iiif/collection/{name}.json")
    logger.info(
        f"SUMMARY: Processing done. Parsed {CustomFormatter.BLUE}{len(metadata)}{CustomFormatter.RESET} "
        f"items and created {CustomFormatter.GREEN}{len(manifests)}{CustomFormatter.RESET} IIIF manifests. "
        f"Items {CustomFormatter.RED}{errors}{CustomFormatter.RESET} were skipped, likely due to issues with "
        f"the images or metadata. Inspect the log above for more details."
    )
    print(random.choice(manifests).json(indent=2))

    # invalidate_cache("/*")
