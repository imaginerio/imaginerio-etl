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
from ..utils.helpers import geo_to_world_coors, get_vocabulary, load_xls, query_wikidata
from ..utils.logger import logger


def update(metadata):

    metadata.fillna("", inplace=True)
    vocabulary = get_vocabulary(VOCABULARY)

    gis = GIS(
        url="https://www.arcgis.com",
        username=ARCGIS_USER,
        password=ARCGIS_PASSWORD,
    )

    viewcones_layer = FeatureLayer(
        VIEWCONES_LAYER_URL,
        gis,
    )

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
            dest = KMLS_OUT if feature.properties.get("ss_id") else KMLS_IN
            if identifier in features:
                logger.warning(
                    f"Object {identifier} is duplicated, will use the last one available"
                )
            features[identifier] = feature
            individual = KML.to_element()
            individual.append(photo_overlay.to_element())
            etree.ElementTree(individual).write(
                f"{dest}/{identifier}.kml", pretty_print=True
            )
        os.remove(item)

    if features:
        geojson_feature_collection = geojson.FeatureCollection(
            features=[
                feature
                for feature in features.values()
                if feature["properties"].get("ss_id")
            ]
        )

        with open(GEOJSON, "w", encoding="utf8") as f:
            json.dump(
                geojson_feature_collection, f, ensure_ascii=False, allow_nan=False
            )

        data_item = gis.content.add(
            item_properties={
                "title": "Viewcones",
                "type": "GeoJson",
                "overwrite": True,
                # "fileName": "viewcones.geojson",
            },
            data=GEOJSON,
        )

        viewcones_layer.append(
            item_id=data_item.id,
            upload_format="geojson",
            upsert=True,
            upsert_matching_field="ss_id",
            update_geometry=True,
        )

        data_item.delete()

    features = viewcones_layer.query(
        where="1=1", out_fields="ss_id", return_geometry=False
    ).features
    viewcones_ssids = {feature.attributes["ss_id"] for feature in features}
    jstor_ssids = set(metadata.loc[metadata["Status"] == "In imagineRio"].index)

    not_in_arcgis = jstor_ssids.difference(viewcones_ssids)
    not_marked_as_ready = viewcones_ssids.difference(jstor_ssids)
    return {"not_in_arcgis": not_in_arcgis, "not_marked_as_ready": not_marked_as_ready}
