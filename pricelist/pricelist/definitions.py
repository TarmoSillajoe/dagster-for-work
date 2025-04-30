from dagster import Definitions, load_assets_from_modules
from dagster_embedded_elt.dlt import DagsterDltResource
from dagster_duckdb import DuckDBResource
from . import assets

dlt_resource = DagsterDltResource()

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    resources={
        "dlt": dlt_resource,
        "database": DuckDBResource(
            database="./pricelist_pipeline.duckdb",
        ),
    },
)
