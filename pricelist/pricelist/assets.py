from dagster import AssetExecutionContext
from dagster_embedded_elt.dlt import DagsterDltResource, dlt_assets
from dlt import pipeline
from .pricelist_pipeline import excel_source
import dagster as dg
import polars as pl


@dg.asset
def pricelist_polars_dataframe():
    result: pl.DataFrame = pl.read_excel(
        "/home/tarmo/Downloads/auger_pricelist.xlsx",
        engine="calamine",
    )
    num_rows = len(result)
    return dg.MaterializeResult(metadata={"number of records": num_rows})


@dlt_assets(
    dlt_source=excel_source(),
    dlt_pipeline=pipeline(
        pipeline_name="pricelist_pipeline",
        dataset_name="pricelist_data",
        destination="duckdb",
    ),
    name="pricelist",
    group_name="pricelist",
)
def dagster_dlt_asset(context: AssetExecutionContext, dlt: DagsterDltResource):
    yield from dlt.run(context=context)
