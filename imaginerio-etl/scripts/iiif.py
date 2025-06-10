import os
import json
from ..config import VOCABULARY, RETILE
from ..entities.item import Item
from ..utils.helpers import get_collections, get_vocabulary, upload_object_to_s3
from ..utils.logger import CustomFormatter as cf
from ..utils.logger import logger


def update(metadata, testing=False):
    n_items = len(metadata)
    logger.info(f"IIIF: {cf.GREEN}{n_items}{cf.RESET} to process")
    vocabulary = get_vocabulary(VOCABULARY)
    collections = get_collections(metadata) if not testing else {}
    n_manifests = 0
    errors = []
    no_collection = metadata.loc[metadata["Collection"].isna()].index.to_list()

    for index, (id, row) in enumerate(metadata.fillna("").iterrows()):
        logger.info(f"{cf.LIGHT_BLUE}{index+1}/{n_items}{cf.BLUE} - Parsing item {id}")
        try:
            item = Item(id, row, vocabulary)
            sizes = item.get_sizes()
            if not sizes or testing or RETILE == "true": # github action input, not boolean
                sizes = item.tile_image()
            manifest = item.create_manifest(sizes)
            
            if testing:
                os.makedirs(f"iiif/{item._id}", exist_ok=True)
                with open(f"iiif/{item._id}/manifest.json", 'w', encoding="utf-8") as f:
                    f.write(manifest.json(indent=4))
                logger.info(f"Saved manifest locally to iiif/{item._id}/manifest.json")
            else:
                upload_object_to_s3(manifest.json(), item._id, f"iiif/{item._id}/manifest.json")
                for name in item.get_collections():
                    collection = collections[name]
                    collection.items = [
                        ref for ref in collection.items if ref.id != manifest.id
                    ]
                    collection.add_item_by_reference(manifest)
            n_manifests += 1
        except Exception:
            logger.exception(
                f"{cf.RED}Couldn't create manifest for item {item._id}, skipping"
            )
            errors.append(item._id)

    if not testing:
        for name in collections.keys():
            upload_object_to_s3(
                collections[name].json(), name, f"iiif/collection/{name.lower()}.json"
            )

    return {
        "n_manifests": n_manifests,
        "n_items": n_items,
        "no_collection": no_collection,
        "errors": errors,
    }
