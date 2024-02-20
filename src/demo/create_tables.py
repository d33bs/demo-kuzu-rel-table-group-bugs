import pathlib
import shutil

import kuzu
from functions import (
    drop_table_if_exists,
    gather_table_names_from_parquet_path,
    generate_cypher_table_create_stmt_from_parquet_path,
)

# set data to be used throughout notebook
parquet_metanames_dir = "data/g2c_lite_2.8.4.full.with-metanames.dataset.parquet"

kuzu_dir = parquet_metanames_dir.replace(".parquet", ".kuzu")

dataset_name_to_cypher_table_type_map = {"nodes": "node", "edges": "rel"}
print(f"Kuzu dir: {kuzu_dir}")


# create path for the kuzu database to reside
if pathlib.Path(kuzu_dir).is_dir():
    shutil.rmtree(kuzu_dir)
pathlib.Path(kuzu_dir).mkdir(exist_ok=True)

# init a Kuzu database and connection
db = kuzu.Database(f"{kuzu_dir}")
kz_conn = kuzu.Connection(db)


# +
lookup_func = object()

for path, table_name_column, primary_key in [
    [f"{parquet_metanames_dir}/nodes", "category", "id"],
    [f"{parquet_metanames_dir}/edges", "predicate", None],
]:
    decoded_type = dataset_name_to_cypher_table_type_map[pathlib.Path(path).name]

    for table_name in gather_table_names_from_parquet_path(
        parquet_path=f"{path}/**", column_with_table_name=table_name_column
    ):
        # create metanames / objects using cypher safe name and dir
        cypher_safe_table_name = table_name.split(":")[1]
        parquet_metanames_metaname_base = f"{path}/{cypher_safe_table_name}"

        drop_table_if_exists(kz_conn=kz_conn, table_name=cypher_safe_table_name)

        if decoded_type == "node":
            create_stmt = generate_cypher_table_create_stmt_from_parquet_path(
                parquet_path=parquet_metanames_metaname_base,
                table_type=decoded_type,
                table_name=cypher_safe_table_name,
                table_pkey_parquet_field_name=primary_key,
            )
        elif decoded_type == "rel":
            create_stmt = generate_cypher_table_create_stmt_from_parquet_path(
                parquet_path=parquet_metanames_metaname_base,
                table_type=decoded_type,
                table_name=cypher_safe_table_name,
                table_pkey_parquet_field_name=primary_key,
                rel_table_field_mapping=[
                    str(pathlib.Path(element).name).split("_")
                    for element in list(
                        pathlib.Path(parquet_metanames_metaname_base).glob("*")
                    )
                ],
            )

        print(
            f"Using the following create statement to create table:\n\n{create_stmt}\n\n"
        )
        kz_conn.execute(create_stmt)
