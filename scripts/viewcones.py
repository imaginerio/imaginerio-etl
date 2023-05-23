import sys

sys.path.insert(0, "./classes")

import json

import os


import sys
from operator import index

import geojson


from camera import KML, Folder, PhotoOverlay
from dotenv import load_dotenv
from helpers import geo_to_world_coors, load_xls, query_wikidata
from lxml import etree

from tqdm import tqdm
from turfpy.misc import sector

load_dotenv(override=True)

CAMERA = os.environ["CAMERA"]

if __name__ == "__main__":

    metadata = load_xls(os.environ["JSTOR"], "SSID")
    vocabulary = load_xls(os.environ["VOCABULARY"], "Label (en)")

    master_kml = "/content/master.kml"
    photo_overlays = []
    features = []
    samples = [
        "data/input/kmls/processed_raw/2019-04-12-Davi.kml",
        "data/input/kmls/processed_raw/2019-05-21-Martim.kml",
    ]

    # if os.path.exists(CAMERA):
    #     features = (geojson.load(open(CAMERA))).features
    # else:
    #     features = []

    if os.path.exists(master_kml):
        master_folder = KML(master_kml)._folder
    else:
        master = KML.to_element()
        master_folder = etree.SubElement(master, "Folder")

    # Parse PhotoOverlays
    source = os.environ["KML_FOLDER"]
    for sample in os.listdir(source):
        sample = KML(os.path.join(source, sample))
        if sample._folder is not None:
            folder = Folder(sample._folder)
            for child in folder._children:
                # try:
                photo_overlays.append(PhotoOverlay(child, metadata))
                # except (ValueError):
                #    continue
        else:
            photo_overlays.append(PhotoOverlay(sample._photooverlay, metadata))

    # Manipulate PhotoOverlays
    with tqdm(photo_overlays, desc="Manipulating PhotoOverlays") as pbar:
        for photo_overlay in pbar:
            # if photo_overlay._ssid:
            pbar.set_postfix_str(photo_overlay._id)
            # photo_overlay.update_id(metadata)
            if "relative" in photo_overlay._altitude_mode:
                photo_overlay.correct_altitude_mode()
            if photo_overlay._depicts:
                photo_overlay.get_radius_via_depicted_entities(vocabulary)
            else:
                photo_overlay.get_radius_via_trigonometry()

            # Dispatch data
            feature = photo_overlay.to_feature()
            # if feature.properties:
            features.append(feature)
            # else:
            #    continue
            # print(photo_overlay.to_element())
            master_folder.append(photo_overlay.to_element())
            # else:
            #    continue

        feature_collection = geojson.FeatureCollection(features=features)
        # print(etree.tostring(master))
        etree.ElementTree(master).write("data/output/main.kml", pretty_print=True)
        with open(os.environ["CAMERA"], "w", encoding="utf8") as f:
            # f.seek(0)
            json.dump(feature_collection, f, indent=4, ensure_ascii=False)
            # f.truncate()
        # print(feature_collection)
