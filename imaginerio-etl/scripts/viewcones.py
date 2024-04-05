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

logging.getLogger("PIL").setLevel(logging.WARNING)


def main():

    metadata = load_xls(JSTOR, "SSID").fillna("")
    vocabulary = load_xls(VOCABULARY, "Label (en)")

    photo_overlays = []
    features = {}

    # Parse PhotoOverlays
    source = KMLS_IN
    for sample in os.listdir(source):
        if sample.endswith("kml"):
            path = os.path.join(source, sample)
            sample = KML(path)
            if sample._folder is not None:
                folder = Folder(sample._folder)
                for child in folder._children:
                    photo_overlays.append(PhotoOverlay(child, metadata))
            else:
                photo_overlays.append(PhotoOverlay(sample._photooverlay, metadata))
            os.rename(path, path.replace("kmls", "kmls_old"))
        else:
            continue
    if photo_overlays:
        # Manipulate PhotoOverlays
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
        geojson_feature_collection = geojson.FeatureCollection(
            features=[
                feature
                for feature in features.values()
                if feature["properties"].get("ss_id")
            ]
        )

        gis = GIS(
            url="https://www.arcgis.com",
            username=ARCGIS_USER,
            password=ARCGIS_PASSWORD,
        )

        viewcones_layer = FeatureLayer(
            VIEWCONES_LAYER,
            gis,
        )

        data_item = gis.content.add(
            item_properties={
                "title": "Viewcones",
                "type": "GeoJson",
                "overwrite": True,
                "fileName": "viewcones.geojson",
            },
            data=io.StringIO(json.dumps(geojson_feature_collection)),
        )

        viewcones_layer.append(
            item_id=data_item.id,
            upload_format="geojson",
            upsert=True,
            upsert_matching_field="ss_id",
            update_geometry=True,
        )

        data_item.delete()

        # with open(GEOJSON, "w", encoding="utf8") as f:
        #    json.dump(geojson_feature_collection, f, ensure_ascii=False, allow_nan=False)
    else:
        logger.info("No KMLs to process.")


if __name__ == "__main__":
    main()
