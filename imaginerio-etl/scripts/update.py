import argparse

from ..utils.logger import logger
from . import iiif, viewcones


def main():
    logger.info("Parsing arguments")
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode", "-m", help="run mode", choices=["test", "prod"], default="test"
    )
    parser.add_argument("--index", "-i", nargs="+", help="index to run", default="all")
    parser.add_argument("--retile", "-r", action="store_true", default=False)
    args = parser.parse_args()
    if args.retile and args.index == "all":
        parser.error("The --retile option cannot be used together with --index='all'")
    viewcones.main()
    iiif.main(args)


if __name__ == "__main__":
    main()
