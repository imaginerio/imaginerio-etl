import argparse
from math import *

from ..config import *
from ..entities.item import Item
from ..utils.helpers import get_collections, get_metadata, upload_object_to_s3
from ..utils.logger import CustomFormatter as cf
from ..utils.logger import logger


def main(args):
    if args.retile and args.index == "all":
        parser.error("The --retile option cannot be used together with --index='all'")

    metadata, vocabulary = get_metadata(JSTOR, VOCABULARY, args.index)
    collections = get_collections(metadata, args.index)
    n_manifests = 0
    errors = []
    no_collection = metadata.loc[metadata["Collection"].isna()].index.to_list()

    for index, (id, row) in enumerate(metadata.fillna("").iterrows()):
        logger.info(
            f"{cf.LIGHT_BLUE}{index+1}/{len(metadata)}{cf.BLUE} - Parsing item {id}"
        )
        try:
            item = Item(id, row, vocabulary)
            sizes = item.get_sizes()
            if not sizes or args.retile == True:
                sizes = item.tile_image()
            manifest = item.create_manifest(sizes)
            upload_object_to_s3(manifest, item._id, f"iiif/{item._id}/manifest.json")
            for name in item.get_collections():
                collections[name].add_item_by_reference(manifest)
            n_manifests += 1
        except Exception as e:
            logger.exception(
                f"{cf.RED}Couldn't create manifest for item {item._id}, skipping"
            )
            errors.append(item._id)

    for name in collections.keys():
        upload_object_to_s3(collections[name], name, f"iiif/collection/{name}.json")

    summary = (
        f"SUMMARY: Processing done. Parsed {cf.BLUE}{len(metadata)}{cf.RESET} "
        f"items and created {cf.GREEN}{n_manifests}{cf.RESET} IIIF manifests. "
    )
    if no_collection:
        summary += (
            f"Items {cf.YELLOW}{no_collection}{cf.RESET} aren't associated with any collections. "
            f"Please fill the Collection field in JSTOR so these images are displayed in imagineRio. "
        )
    if errors:
        summary += (
            f"Items {cf.RED}{errors}{cf.RESET} were skipped, likely due to issues with "
            f"the images or metadata. Inspect the log above (with CTRL+F) for more details."
        )
    logger.info(summary)


if __name__ == "__main__":
    logger.info("Parsing arguments")
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode", "-m", help="run mode", choices=["test", "prod"], default="test"
    )
    parser.add_argument("--index", "-i", nargs="+", help="index to run", default="all")
    parser.add_argument("--retile", "-r", action="store_true", default=False)
    args = parser.parse_args()

    main(args)
