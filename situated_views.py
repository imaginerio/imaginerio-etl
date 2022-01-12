from dagster import repository
from jobs.format_ims_metadata import format_ims_metadata
from jobs.handle_images import handle_images
from jobs.query_apis import query_apis
from jobs.export_data import export_data
from jobs.compile_metadata import compile_metadata
from jobs.process_kmls import process_kmls
from jobs.iiif_factory import iiif_factory


@repository
def situated_views():
    return [
        format_ims_metadata,
        handle_images,
        query_apis,
        export_data,
        compile_metadata,
        process_kmls,
        iiif_factory,
    ]
