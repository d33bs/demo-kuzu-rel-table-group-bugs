import pathlib

import kuzu
from functions import kz_execute_with_retries

# set data to be used throughout notebook

parquet_metanames_dir = "data/g2c_lite_2.8.4.full.with-metanames.dataset.parquet"

kuzu_dir = parquet_metanames_dir.replace(".parquet", ".kuzu")

dataset_name_to_cypher_table_type_map = {"nodes": "node", "edges": "rel"}

print(f"Kuzu dir: {kuzu_dir}")


# init a Kuzu database and connection
db = kuzu.Database(f"{kuzu_dir}")
kz_conn = kuzu.Connection(db)


# note: we provide specific ordering here to ensure nodes are created before edges
table_count = 1
sub_table_count = 1
for path in [f"{parquet_metanames_dir}/nodes", f"{parquet_metanames_dir}/edges"]:
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
                    else f'COPY {table_name}_{rel_node_pair_name} FROM "{rel_node_pair}/*.parquet"'
                )
                print(ingest_stmt)
                print(f"Table count: {table_count}, Sub-table count: {sub_table_count}")
                sub_table_count += 1
                kz_execute_with_retries(kz_conn=kz_conn, kz_stmt=ingest_stmt)

            table_count += 1
print("Finished running Kuzu COPY statements.")
