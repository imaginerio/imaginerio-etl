import sys

sys.path.insert(0, "./classes")
import argparse
import json
import logging
import logging.config
import os
import re
import shutil
from math import *

import boto3
import pandas as pd
from dotenv import load_dotenv
from item import BUCKET, COLLECTIONS, Item
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

from helpers import (
    load_xls,
    fast_upload,
    logger,
    session,
    upload_folder_to_s3,
)  # , invalidate_cache

load_dotenv(override=True)


def get_items(metadata, vocabulary, mode):

    os.makedirs(COLLECTIONS, exist_ok=True)

    # list all collection names
    collections = metadata["Collection"].dropna().str.split("\|").explode().unique()
    # download collection(s)
    for name in collections:
        collection_path = COLLECTIONS + name.lower() + ".json"
        response = session.get(BUCKET + collection_path)
        if response.status_code == 200:
            with open(collection_path, "w") as f:
                json.dump(response.json(), f, indent=4)

    # save current and try to load previous data
    # if mode == "prod":
    #     try:
    #         last_run = pd.read_pickle("data/input/last_run.pickle")
    #     except:
    #         last_run = None

    #     # pickle current data to compare against
    #     pd.to_pickle(metadata, "data/input/last_run.pickle")

    #     # process only new or modified items
    #     if last_run is not None:
    #         new_items = metadata.loc[~metadata.index.isin(last_run.index)]
    #         previous_items = metadata.loc[last_run.index]
    #         modified_items = last_run.compare(previous_items).index
    #         to_process = metadata.loc[modified_items].append(new_items)
    #     else:
    #         to_process = metadata
    # else:
    # to_process = metadata

    ##logger.debug(f"Processing {len(to_process)} items")

    return [Item(id, row, vocabulary) for id, row in metadata.fillna("").iterrows()]


if __name__ == "__main__":

    # parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode", "-m", help="run mode", choices=["test", "prod"], default="test"
    )
    parser.add_argument("--index", "-i", help="index to run", default=0)
    args = parser.parse_args()

    if os.path.exists("/iiif"):
        shutil.rmtree("/iiif")

    # open files and rename columns
    metadata = load_xls(os.environ["JSTOR"], "SSID")
    vocabulary = load_xls(os.environ["VOCABULARY"], "Label (en)")

    # filter rows
    if args.mode == "test":
        metadata = pd.DataFrame(metadata.iloc[int(args.index)]).T
    else:
        metadata = metadata.loc[metadata["Status"] == "In imagineRio"]

    # filter columns
    metadata = metadata[
        [
            "Document ID",
            "Rights",
            "Provider",
            "Collection",
            "Type",
            "Material",
            "Fabrication Method",
            "Media URL",
            "Creator",
            "Title",
            "Description (Portuguese)",
            "Description (English)",
            "Date",
            "First Year",
            "Last Year",
            "Document URL",
            "Required Statement",
            "Wikidata ID",
            "Smapshot ID",
            "Depicts",
            "Width",
            "Height",
        ]
    ]

    items = get_items(metadata, vocabulary, args.mode)

    with logging_redirect_tqdm():
        main_pbar = tqdm(items, desc="Creating IIIF assets")
        for item in main_pbar:

            main_pbar.set_postfix_str(str(item._id))
            success = item.write_manifest()

            if success and args.mode == "prod":  # TODO item.upload_and_remove()
                tiles = [
                    os.path.join(root, file)
                    for root, _, files in os.walk(item._base_path)
                    for file in files
                ]
                total_size = sum([os.stat(f).st_size for f in tiles])
                with tqdm(
                    tiles,
                    desc="upload",
                    ncols=60,
                    total=total_size,
                    unit="B",
                    unit_scale=1,
                    leave=False,
                ) as upload_pbar:
                    fast_upload(
                        boto3.Session(), "imaginerio-images", tiles, upload_pbar.update
                    )
                # tqdm.write(f"Upload complete, removing folder {item._base_path}")
                shutil.rmtree(os.path.abspath(item._base_path))
            else:
                continue
        if args.mode == "prod":
            fast_upload(
                boto3.Session(),
                "imaginerio-images",
                [os.path.relpath(file) for file in os.listdir(COLLECTIONS)],
            )
            # upload_folder_to_s3(COLLECTIONS, mode=args.mode)

    # invalidate_cache("/*")
