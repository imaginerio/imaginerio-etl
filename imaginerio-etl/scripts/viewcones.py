import io
import json
import logging
import os
import sys
from operator import index

import geojson
from arcgis import GIS
from arcgis.features import FeatureLayer
from lxml import etree
from turfpy.misc import sector

from ..config import *
from ..entities.camera import KML, Folder, PhotoOverlay
from ..utils.helpers import geo_to_world_coors, load_xls, query_wikidata
from ..utils.logger import logger


def main():

    metadata = load_xls(JSTOR, "SSID").fillna("")
    vocabulary = load_xls(VOCABULARY, "Label (en)")

    features = {}

    # Parse PhotoOverlays
    for item in [
        os.path.join(KMLS_IN, filename)
        for filename in os.listdir(KMLS_IN)
        if filename.endswith("kml")
    ]:
        photo_overlays = []
        kml = KML(item)
        if kml._folder is not None:
            folder = Folder(kml._folder)
            for child in folder._children:
                photo_overlays.append(PhotoOverlay(child, metadata))
        else:
            photo_overlays.append(PhotoOverlay(kml._photooverlay, metadata))

        for photo_overlay in photo_overlays:
            # logger.debug(f"Processing image {index}/{len(photo_overlays)}")
            if "relative" in photo_overlay._altitude_mode:
                photo_overlay.correct_altitude_mode()

            if photo_overlay._depicts:
                photo_overlay.get_radius_via_depicted_entities(vocabulary)
            else:
                photo_overlay.get_radius_via_trigonometry()

            # Dispatch data
            feature = photo_overlay.to_feature()
            identifier = feature.properties.get("ss_id") or feature.properties.get(
                "document_id"
            )
            if identifier in features:
                logger.warning(
                    f"Object {identifier} is duplicated, will use the last one available"
                )
            features[identifier] = feature
            individual = KML.to_element()
            individual.append(photo_overlay.to_element())
            etree.ElementTree(individual).write(
                f"{KMLS_OUT}/{identifier}.kml", pretty_print=True
            )
        os.remove(item)

    geojson_feature_collection = geojson.FeatureCollection(
        features=[
            feature
            for feature in features.values()
            if feature["properties"].get("ss_id")
        ]
    )

    with open("data/output/viewcones.geojson", "w", encoding="utf8") as f:
        json.dump(geojson_feature_collection, f, ensure_ascii=False, allow_nan=False)

    gis = GIS(
        url="https://www.arcgis.com",
        username=ARCGIS_USER,
        password=ARCGIS_PASSWORD,
    )

    viewcones_layer = FeatureLayer(
        VIEWCONES_LAYER_URL,
        gis,
    )

    data_item = gis.content.add(
        item_properties={
            "title": "Viewcones",
            "type": "GeoJson",
            "overwrite": True,
            # "fileName": "viewcones.geojson",
        },
        data="data/output/viewcones.geojson",
    )

    viewcones_layer.append(
        item_id=data_item.id,
        upload_format="geojson",
        upsert=True,
        upsert_matching_field="ss_id",
        update_geometry=True,
    )

    data_item.delete()


if __name__ == "__main__":
    main()
