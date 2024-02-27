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
from ..utils.helpers import (  # , invalidate_cache
    create_collection,
    fast_upload,
    load_xls,
    logger,
    session,
    upload_folder_to_s3,
)


def get_items(metadata, vocabulary):
    logger.debug("Creating objects")
    return [Item(id, row, vocabulary) for id, row in metadata.fillna("").iterrows()]


def get_collections(metadata):
    collections = {}
    # list all collection names
    labels = metadata["Collection"].dropna().str.split("|").explode().unique()
    # create collection(s)
    for label in labels:
        collection = create_collection(label)
        collections[label] = collection

    return collections


def get_metadata(metadata_path, vocabulary_path):
    logger.debug("Loading metadata files")
    # open files and rename columns
    metadata = load_xls(metadata_path, "SSID")
    vocabulary = load_xls(vocabulary_path, "Label (en)").to_dict("index")

    logger.debug("Filtering items")
    # filter rows
    if args.mode != "all":
        metadata = pd.DataFrame(metadata.iloc[int(args.index)]).T
    else:
        metadata = metadata.loc[metadata["Status"] == "In imagineRio"]

    return metadata, vocabulary


if __name__ == "__main__":
    logger.debug("Parsing arguments")
    # parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode", "-m", help="run mode", choices=["test", "prod"], default="test"
    )
    parser.add_argument("--index", "-i", help="index to run", default=0)
    args = parser.parse_args()

    metadata, vocabulary = get_metadata(JSTOR, VOCABULARY)
    collections = get_collections(metadata)
    manifests = []
    # items = get_items(metadata, vocabulary)

    with logging_redirect_tqdm(loggers=[logger]):
        main_pbar = tqdm(
            metadata.fillna("").iterrows(),
            total=len(metadata),
            desc="Creating IIIF assets",
        )
        for id, row in main_pbar:
            item = Item(id, row, vocabulary)
            main_pbar.set_postfix_str(str(item._id))
            sizes = item.get_sizes() or item.tile_image()
            manifest = item.create_manifest(sizes)
            if manifest:
                manifests.append(manifest)
                for name in item.get_collections():
                    collections[name].add_item_by_reference(manifest)
            else:
                logger.warning(
                    f"Couldn't create manifest for item {item._id}, skipping"
                )

        print(random.choice(manifests).json(indent=2))
        if args.mode == "prod":
            logger.info("Uploading collections to S3")
            # fast_upload(
            #     boto3.Session(),
            #     "imaginerio-images",
            #     [os.path.relpath(file) for file in os.listdir(COLLECTIONS)],
            # )
            # upload_folder_to_s3(COLLECTIONS, mode=args.mode)

    # invalidate_cache("/*")
