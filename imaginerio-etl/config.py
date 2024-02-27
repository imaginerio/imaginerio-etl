import os

from dotenv import load_dotenv

# load_dotenv(override=True)

IMS_METADATA = os.getenv("IMS_METADATA")
IMS2JSTOR = os.getenv("IMS2JSTOR")
REVIEW = os.getenv("REVIEW")
# JSTOR = os.getenv("JSTOR")
# VOCABULARY = os.getenv("VOCABULARY")
# BUCKET = os.getenv("BUCKET")
# CLOUDFRONT = os.getenv("CLOUDFRONT")
DISTRIBUTION_ID = os.getenv("DISTRIBUTION_ID")

JSTOR = "data/input/jstor.xls"
VOCABULARY = "data/input/vocabulary.xls"
CLOUDFRONT = "https://iiif.imaginerio.org/iiif"
