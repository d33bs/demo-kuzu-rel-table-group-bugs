import pathlib
import time
from typing import List, Literal, Optional

import kuzu
from pyarrow import parquet


def kz_execute_with_retries(
    kz_conn: kuzu.connection.Connection, kz_stmt: str, retry_count: int = 5
):
    """
    Retry running a kuzu execution up to retry_count number of times.
    """

    while retry_count > 1:
        try:
            kz_conn.execute(kz_stmt)
            break
        except RuntimeError as runexc:
            # catch previous copy work and immediately move on
            if (
                str(runexc)
                == "Copy exception: COPY commands can only be executed once on a table."
            ):
                print(runexc)
                break
            elif "Unable to find primary key value" in str(runexc):
                print(f"Retrying after primary key exception: {runexc}")
                # wait a half second before attempting again
                time.sleep(0.5)
                retry_count -= 1
            else:
                raise


def ingest_data_to_kuzu_tables(
    parquet_dir: str,
    kz_conn: kuzu.connection.Connection,
    dataset_name_to_cypher_table_type_map: Dict[str, str],
):
    # note: we provide specific ordering here to ensure nodes are created before edges
    table_count = 1
    sub_table_count = 1
    for path in [f"{parquet_dir}/nodes", f"{parquet_dir}/edges"]:
        decoded_type = dataset_name_to_cypher_table_type_map[pathlib.Path(path).name]
        print(f"Working on kuzu ingest of parquet dataset: {path} ")
        for table in pathlib.Path(path).glob("*"):
            table_name = table.name
            if decoded_type == "node":
                # uses wildcard functionality for all files under parquet dataset dir
                # see: https://kuzudb.com/docusaurus/data-import/csv-import#copy-from-multiple-csv-files-to-a-single-table
                ingest_stmt = f'COPY {table_name} FROM "{table}/*.parquet"'
                print(ingest_stmt)
                table_count += 1
                print(f"Table count: {table_count}")
                kz_execute_with_retries(kz_conn=kz_conn, kz_stmt=ingest_stmt)
            elif decoded_type == "rel":
                rel_node_pairs = list(pathlib.Path(table).glob("*"))

                sub_table_count = 1
                for rel_node_pair in rel_node_pairs:
                    rel_node_pair_name = rel_node_pair.name

                    ingest_stmt = (
                        f'COPY {table_name} FROM "{rel_node_pair}/*.parquet"'
                        if len(rel_node_pairs) == 1
                        else f'COPY {table_name}_{rel_node_pair_name}" FROM "{rel_node_pair}/*.parquet"'
                    )
                    print(ingest_stmt)
                    print(
                        f"Table count: {table_count}, Sub-table count: {sub_table_count}"
                    )
                    sub_table_count += 1
                    kz_execute_with_retries(kz_conn=kz_conn, kz_stmt=ingest_stmt)

                table_count += 1
    print("Finished running Kuzu COPY statements.")
