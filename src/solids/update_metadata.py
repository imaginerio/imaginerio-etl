import dagster as dg
from tests.dataframe_types import *


@dg.solid(
    input_defs=[dg.InputDefinition(
        "metadata", root_manager_key="metadata_root")],
    output_defs=[
        dg.OutputDefinition(
            io_manager_key="pandas_csv", name="metadata", dagster_type=dp.DataFrame
        )
    ],
)
def update_metadata(
    context, df: main_dataframe_types, metadata: metadata_dataframe_types
):
    """
    Overwrite metadata.csv with newly processed data
    """
    df.reset_index(inplace=True)
    # find items how not are found on metadata
    filter = df["Source ID"].isin(metadata["Source ID"])
    review = list(df["Source ID"].loc[~filter])
    context.log.info(f"{len(review)} Items to review: {review}")

    df.set_index("Source ID", inplace=True)

    metadata.set_index("Source ID", inplace=True)
    metadata.update(df)

    return metadata
