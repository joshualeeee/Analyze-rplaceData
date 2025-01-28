import pandas as pd
import webcolors
import pyarrow.csv as pv
import pyarrow.parquet as pq
import pyarrow as pa
import polars as pl

def csv_to_parquet_chunks():
    # Read the CSV file in chunks (for memory efficiency with large files)
    csv_file = '../../2022_place_canvas_history.csv'
    parquet_file = 'rPlace.parquet'  # Output Parquet file path

    DATESTRING_FORMAT = "%Y-%m-%d %H:%M:%S"
    BLOCK_SIZE = 100_000_000

    read_options = pv.ReadOptions(block_size=BLOCK_SIZE)
    csv_reader = pv.open_csv(csv_file, read_options=read_options)

    parquet_writer = None
    user_id_mapping = {}
    user_id_counter = 0

    try:
        for i, record_batch in enumerate(csv_reader):
            print(f"Processing batch with {record_batch.num_rows} rows...")

            df = pl.from_arrow(record_batch)

            df = df.with_columns(
                pl.col("timestamp")
                .str.replace(r" UTC$", "")  
                .str.strptime(
                    pl.Datetime, 
                    format="%Y-%m-%d %H:%M:%S%.f",
                    strict=False
                )
                .alias("timestamp")
            )

            # Map user_id to numerical values
            user_ids = df["user_id"].unique().to_list()
            for user_id in user_ids:
                if user_id not in user_id_mapping:
                    user_id_mapping[user_id] = user_id_counter
                    user_id_counter += 1

            df = (
                df.filter(
                    pl.col("coordinate").str.count_matches(",") == 1
                )
                .with_columns(
                    pl.col("coordinate")
                    .str.split_exact(",", 1)
                    .struct.field("field_0")
                    .cast(pl.Int64)
                    .alias("x"),
                    pl.col("coordinate")
                    .str.split_exact(",", 1)
                    .struct.field("field_1")
                    .cast(pl.Int64)
                    .alias("y"),
                )
                .drop("coordinate")
                )

            # map column for numerical ids
            df = df.with_columns(
                df["user_id"].map_elements(lambda x: user_id_mapping.get(x, None), return_dtype=pl.Int64).alias("user_id_numerical")
            )

            df = df.drop("user_id")

            table = df.to_arrow()

            if parquet_writer is None:
                parquet_writer = pq.ParquetWriter(
                    parquet_file, 
                    schema=table.schema, 
                    compression="zstd"
                )
            parquet_writer.write_table(table)

    finally:
        if parquet_writer:
            parquet_writer.close()

    print(f"Successfully converted {csv_file} to {parquet_file}")


if __name__ == "__main__":
    csv_to_parquet_chunks()
