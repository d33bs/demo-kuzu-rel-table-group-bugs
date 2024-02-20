import kuzu
from pyarrow import parquet


def drop_table_if_exists(kz_conn: kuzu.connection.Connection, table_name: str):
    try:
        kz_conn.execute(f"DROP TABLE {table_name}")
    except Exception as e:
        print(e)
        print("Warning: no need to drop table.")


def gather_table_names_from_parquet_path(
    parquet_path: str,
    column_with_table_name: str = "id",
):
    # return distinct table types as set comprehension
    return set(
        # create a parquet dataset and read a single column as an array
        parquet.ParquetDataset(parquet_path)
        .read(columns=[column_with_table_name])[column_with_table_name]
        .to_pylist()
    )


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


def generate_cypher_table_create_stmt_from_parquet_path(
    parquet_path: str,
    table_type: Literal["node", "rel"],
    table_name: str,
    rel_table_field_mapping: Optional[List[str]] = None,
    table_pkey_parquet_field_name: str = "id",
):

    if pathlib.Path(parquet_path).is_dir():
        # use first file discovered as basis for schema
        parquet_path = next(pathlib.Path(parquet_path).rglob("*.parquet"))

    parquet_schema = parquet.read_schema(parquet_path)

    # Map Parquet data types to Cypher data types
    # more details here: https://kuzudb.com/docusaurus/cypher/data-types/
    parquet_to_cypher_type_mapping = {
        "string": "STRING",
        "int32": "INT32",
        "int64": "INT64",
        "number": "FLOAT",
        "float": "FLOAT",
        "double": "FLOAT",
        "boolean": "BOOLEAN",
        "object": "MAP",
        "array": "INT64[]",
        "list<element: string>": "STRING[]",
        "null": "NULL",
        "date": "DATE",
        "time": "TIME",
        "datetime": "DATETIME",
        "timestamp": "DATETIME",
        "any": "ANY",
    }

    # Generate Cypher field type statements
    cypher_fields_from_parquet_schema = ", ".join(
        [
            # note: we use string splitting here for nested types
            # for ex. list<element: string>
            f"{field.name} {parquet_to_cypher_type_mapping.get(str(field.type))}"
            for idx, field in enumerate(parquet_schema)
            if table_type == "node" or (table_type == "rel" and idx > 1)
        ]
    )

    # branch for creating node table
    if table_type == "node":

        if table_pkey_parquet_field_name not in [
            field.name for field in parquet_schema
        ]:
            raise LookupError(
                f"Unable to find field {table_pkey_parquet_field_name} in parquet file {parquet_path}."
            )

        return (
            f"CREATE NODE TABLE {table_name}"
            f"({cypher_fields_from_parquet_schema}, "
            f"PRIMARY KEY ({table_pkey_parquet_field_name}))"
        )

    # else we return for rel tables
    # single or as a group, see here for more:
    # https://kuzudb.com/docusaurus/cypher/data-definition/create-table/#create-rel-table-group

    # compile string version of the node type possibilities for kuzu rel group
    subj_and_objs = ", ".join(
        [f"FROM {subj} TO {obj}" for subj, obj in rel_table_field_mapping]
    )

    rel_tbl_start = (
        f"CREATE REL TABLE {table_name}"
        if len(rel_table_field_mapping) == 1
        else f"CREATE REL TABLE GROUP {table_name}"
    )

    return f"{rel_tbl_start} ({subj_and_objs}, {cypher_fields_from_parquet_schema})"
