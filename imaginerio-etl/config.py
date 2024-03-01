import os

JSTOR = "data/input/jstor.xls"
VOCABULARY = "data/input/vocabulary.xls"
GEOJSON = "data/output/viewcones.geojson"
CLOUDFRONT = "https://iiif.imaginerio.org/iiif"
BUCKET = "https://imaginerio-images.s3.us-east-1.amazonaws.com/"
DISTRIBUTION_ID = os.getenv("DISTRIBUTION_ID")
# AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
# AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

# IMS_METADATA = os.getenv("IMS_METADATA")
# IMS2JSTOR = os.getenv("IMS2JSTOR")
# REVIEW = os.getenv("REVIEW")

RIGHTS = {
    "Copyright Not Evaluated": "http://rightsstatements.org/vocab/CNE/1.0/",
    "Copyright Undetermined": "http://rightsstatements.org/vocab/UND/1.0/",
    "In Copyright": "http://rightsstatements.org/vocab/InC/1.0/",
    "In Copyright - Educational Use Permitted": "http://rightsstatements.org/vocab/InC-EDU/1.0/",
    "In Copyright - EU Orphan Work": "http://rightsstatements.org/vocab/InC-OW-EU/1.0/",
    "In Copyright - Non-Commercial Use Permitted": "http://rightsstatements.org/vocab/InC-NC/1.0/",
    "In Copyright - Unknown Rightsholder": "http://rightsstatements.org/vocab/InC-RUU/1.0/",
    "No Copyright - Contractual Restrictions": "http://rightsstatements.org/vocab/NoC-CR/1.0/",
    "No Copyright - Non-Commercial Use Only": "http://rightsstatements.org/vocab/NoC-NC/1.0/",
    "No Copyright - Other Known Legal Restrictions": "http://rightsstatements.org/vocab/NoC-OKLR/1.0/",
    "No Copyright - Uniteed States": "http://rightsstatements.org/vocab/NoC-US/1.0/",
    "No Known Copyright": "http://rightsstatements.org/vocab/NKC/1.0/",
    "Public Domain": "https://creativecommons.org/publicdomain/mark/1.0/",
}
