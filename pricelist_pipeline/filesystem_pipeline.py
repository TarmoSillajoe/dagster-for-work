from typing import Iterator

import dlt
from dlt.common.storages.fsspec_filesystem import FileItemDict
from dlt.common.typing import TDataItems
from dlt.sources.filesystem import filesystem

BUCKET_URL = "file:/home/tarmo/Downloads"


@dlt.transformer(standalone=True)
def read_excel(
    items: Iterator[FileItemDict], sheet_id: int = 1
) -> Iterator[TDataItems]:
    import polars as pl

    for file_obj in items:
        with file_obj.open("rb") as file:
            file_content = file.read()
            yield pl.read_excel(
                file_content,
                engine="calamine",
            ).to_dicts()


@dlt.resource(write_disposition="append")
def excel_resource():
    return (
        filesystem(bucket_url=BUCKET_URL, file_glob="auger_pricelist.xlsx")
        | read_excel()
    )


@dlt.transformer(standalone=True)
def read_xml(items: Iterator[FileItemDict]) -> Iterator[TDataItems]:
    import xmltodict

    for file_obj in items:
        with file_obj.open() as file:
            yield xmltodict.parse(file.read())


@dlt.resource(write_disposition="append")
def xml_resource():
    return filesystem(bucket_url=BUCKET_URL, file_glob="*.xml") | read_xml()


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="autos_invoice_pipeline",
        destination="duckdb",
        dataset_name="autos_invoice",
    )
    # load_info = pipeline.run(auger_xlsx.with_name("auger_pricelist"))
    load_info = pipeline.run([xml_resource()])
    print(load_info)
