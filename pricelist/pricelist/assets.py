from dagster import AssetExecutionContext
from dagster_embedded_elt.dlt import DagsterDltResource, dlt_assets
from dlt import pipeline
from .pricelist_pipeline import excel_source
import dagster as dg
import polars as pl
from dagster_duckdb import DuckDBResource


# @dg.asset
@dg.asset
def koivunen_pricelist_duckdb(database: DuckDBResource):
    bucket_url = "/mnt/c/Users/tarmos/OneDrive - BALTI AUTOOSAD AS/koivunen/"

    with database.get_connection() as conn:
        conn.execute(
            f"""
            create or replace table koivunen_staged as
            from 
                read_csv('{bucket_url}/basic_info.csv',
                    sep='|',
                    normalize_names=true,
                    null_padding=true,
                    ignore_errors=true,
                    decimal_separator=',',
                    types={{ 'ean_code': varchar }}
                        );
            """
        )


@dg.asset
def pricelist_polars_dataframe():
    result: pl.DataFrame = pl.read_excel(
        "/mnt/c/Users/tarmos/OneDrive - BALTI AUTOOSAD AS/auger/Balti Autoosad AS.XLSX",
        engine="calamine",
    )
    num_rows = len(result)
    return dg.MaterializeResult(metadata={"number of records": num_rows})


@dlt_assets(
    dlt_source=excel_source(),
    dlt_pipeline=pipeline(
        pipeline_name="pricelist_pipeline",
        dataset_name="staging",
        destination="duckdb",
    ),
    name="pricelist",
    group_name="pricelist",
)
def dagster_dlt_asset(context: AssetExecutionContext, dlt: DagsterDltResource):
    yield from dlt.run(context=context)
