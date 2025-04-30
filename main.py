import duckdb
import polars as pl


def main():
    print("Hello from dagster-dlt-project!")


if __name__ == "__main__":
    # basic_info.csv"
    bucket_url = "/mnt/c/Users/tarmos/OneDrive - BALTI AUTOOSAD AS/koivunen/"

    with duckdb.connect(":memory") as conn:
        df = conn.query(
            f"""
            select
                *
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
        ).pl()
    pl.Config.set_tbl_cols(40)
    pl.Config.set_tbl_rows(40)
    print(df.sample(40))
