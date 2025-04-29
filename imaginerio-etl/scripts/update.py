import argparse
import os

from ..config import (
    CURRENT_JSTOR,
    NEW_JSTOR,
    KMLS_IN
)
from ..utils.helpers import get_metadata_changes, summarize, load_xls
from ..utils.logger import logger
from . import iiif, viewcones


def main():
    parser = argparse.ArgumentParser(description="Run JSTOR ETL update process.")
    parser.add_argument("--id", help="Process only the row with this index from NEW_JSTOR")
    args = parser.parse_args()

    if args.id: # Run a single item for testing 
        try:
            changed_data = load_xls(NEW_JSTOR, "SSID").loc[[args.id]]
            logger.info(f"Running ETL on row with ID: {args.id}")
        except KeyError:
            logger.error(f"ID '{args.id}' not found in NEW_JSTOR index.")
            return
    else: # Compare data, overwrite current data file if there are changes
        all_data, changed_data = get_metadata_changes(CURRENT_JSTOR, NEW_JSTOR)

    # Update viewcones if any
    if any(file for file in os.listdir(KMLS_IN) if file != ".gitkeep"):
        viewcones_info = viewcones.update(all_data)
    else:
        logger.info("No KMLs to process, skipping")
        viewcones_info = None

    # Update manifests if published items data has changed
    if changed_data.empty:
        logger.info("No metadata changes detected, exiting")
        manifest_info = None
    else:
        manifest_info = iiif.update(changed_data, testing=args.id is not None)

    if viewcones_info or manifest_info:
        summary = summarize(viewcones_info, manifest_info)
        logger.info(summary)


if __name__ == "__main__":
    main()
