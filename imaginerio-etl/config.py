import os
from typing import Dict, Any

# File paths and directories
CURRENT_JSTOR = "data/input/jstor.xls"
NEW_JSTOR = "data/jstor_download/jstor.xls"
VOCABULARY = "data/input/vocabulary.xls"
ITEMS_TO_PROCESS = "data/input/items_to_process.xls"
KMLS_IN = "data/input/kmls"
KMLS_OUT = "data/output/kmls"
GEOJSON = "data/output/viewcones.geojson"

# URLs and endpoints
CLOUDFRONT = "https://iiif.imaginerio.org/iiif"
BUCKET = "https://imaginerio-images.s3.us-east-1.amazonaws.com/"
BUCKET_NAME = "imaginerio-images"

# Environment variables
DISTRIBUTION_ID = os.getenv("DISTRIBUTION_ID")
ARCGIS_USER = os.getenv("ARCGIS_USER")
ARCGIS_PASSWORD = os.getenv("ARCGIS_PASSWORD")
ARCGIS_PORTAL = os.getenv("ARCGIS_PORTAL")
VIEWCONES_LAYER_URL = os.getenv("VIEWCONES_LAYER_URL")
RETILE = os.getenv("RETILE", False)
REPROCESS = os.getenv("REPROCESS", False)

# Metadata field names
class MetadataFields:
    TITLE = "Title"
    DESC_EN = "Description (English)"
    DESC_PT = "Description (Portuguese)"
    DOCUMENT_ID = "Document ID"
    CREATOR = "Creator"
    DATE = "Date"
    DEPICTS = "Depicts"
    TYPE = "Type"
    MATERIAL = "Material"
    FABRICATION_METHOD = "Fabrication Method"
    WIDTH = "Width"
    HEIGHT = "Height"
    REQUIRED_STATEMENT = "Required Statement"
    RIGHTS = "Rights"
    DOCUMENT_URL = "Document URL"
    PROVIDER = "Provider"
    WIKIDATA_ID = "Wikidata ID"
    SMAPSHOT_ID = "Smapshot ID"
    COLLECTION = "Collection"
    MEDIA_URL = "Media URL"
    TRANSCRIPTION = "Transcription - Original Language"
    TRANSCRIPTION_EN = "Transcription - English Translation"
    TRANSCRIPTION_PT = "Transcription - Portuguese Translation"

# Vocabulary field names
class VocabularyFields:
    WIKIDATA_ID = "Wikidata ID"
    LABEL_PT = "Label (pt)"
    LABEL_EN = "Label (en)"
    URL = "URL"

# Language codes
class Languages:
    EN = "en"
    PT_BR = "pt-BR"
    NONE = "none"

# IIIF manifest configuration
class IIIFConfig:
    PROVIDER_ID = "https://imaginerio.org/"
    PROVIDER_LABEL = "imagineRio"
    LOGO_URL = "https://forum.imaginerio.org/uploads/default/original/1X/8c4f71106b4c8191ffdcafb4edeedb6f6f58b482.png"
    LOGO_HEIGHT = 164
    LOGO_WIDTH = 708
    DEFAULT_RIGHTS = "http://rightsstatements.org/vocab/CNE/1.0/"
    IMAGE_SERVICE_PROFILE = "level0"
    IMAGE_FORMAT = "image/jpeg"
    TEXT_FORMAT = "text/html"

# Rights statements mapping
RIGHTS: Dict[str, str] = {
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
    "No Copyright - United States": "http://rightsstatements.org/vocab/NoC-US/1.0/",
    "No Known Copyright": "http://rightsstatements.org/vocab/NKC/1.0/",
    "Public Domain": "https://creativecommons.org/publicdomain/mark/1.0/",
}
