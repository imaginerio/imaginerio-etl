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
    vocabulary = load_xls(vocabulary_path)

    logger.debug("Filtering items")
    # filter rows
    if args.mode == "test":
        if isinstance(args.index, int):
            metadata = pd.DataFrame(metadata.iloc[int(args.index)]).T
        elif args.index == "all":
            pass
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
            manifest = item.create_manifest()
            if manifest:
                for name in item.get_collections():
                    collections[name].add_item_by_reference(manifest)

            # if success and args.mode == "prod":  # TODO item.upload_and_remove()
            #     item_data = [
            #         os.path.join(root, file)
            #         for root, _, files in os.walk(item._base_path)
            #         for file in files
            #     ]
            #     total_size = sum([os.stat(f).st_size for f in item_data])
            #     logger.info(f"Uploading item {item._id} to S3")
            #     with tqdm(
            #         item_data,
            #         desc="upload",
            #         ncols=60,
            #         total=total_size,
            #         unit="B",
            #         unit_scale=1,
            #         leave=False,
            #     ) as upload_pbar:
            #         fast_upload(
            #             boto3.Session(),
            #             "imaginerio-images",
            #             item_data,
            #             upload_pbar.update,
            #         )
            #     # tqdm.write(f"Upload complete, removing folder {item._base_path}")
            #     shutil.rmtree(os.path.abspath(item._base_path))
            else:
                continue
        print(random.choice(random.choice(list(collections.values())).items))
        if args.mode == "prod":
            logger.info("Uploading collections to S3")
            # fast_upload(
            #     boto3.Session(),
            #     "imaginerio-images",
            #     [os.path.relpath(file) for file in os.listdir(COLLECTIONS)],
            # )
            # upload_folder_to_s3(COLLECTIONS, mode=args.mode)

    # invalidate_cache("/*")
