from typing import Iterator
import dlt
from dlt.common.storages.fsspec_filesystem import FileItemDict
from dlt.common.typing import TDataItems
from dlt.sources.filesystem import filesystem

BUCKET_URL = "/mnt/c/Users/tarmos/OneDrive - BALTI AUTOOSAD AS/auger/"


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


@dlt.transformer(standalone=True)
def read_xml(items: Iterator[FileItemDict]) -> Iterator[TDataItems]:
    import xmltodict

    for file_obj in items:
        with file_obj.open() as file:
            yield xmltodict.parse(file.read())


@dlt.resource(write_disposition="append")
def excel_resource():
    return (
        filesystem(bucket_url=BUCKET_URL, file_glob="Balti Autoosad AS.XLSX")
        | read_excel()
    )


@dlt.resource(write_disposition="append")
def xml_resource():
    return filesystem(bucket_url=BUCKET_URL, file_glob="*.xml") | read_xml()


@dlt.source
def excel_source():
    return [
        excel_resource(),
    ]
