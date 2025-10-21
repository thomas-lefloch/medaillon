# Vérifie que les données sont valides.
# On utilise great_expectations pour la validation.
# Le nettoyage et la sauvegarde des données et ensuite réalisé par pandas.
# Alternative: on aurait pu utiliser great_expectations actions
# mais ça à l'air trop compliquer pour ce que ca fait. Cela reste une alternative à explorer

import great_expectations as gx
import pandas as pd
import argparse


# returns the list of indexes of invalid rows
def validate_nodes(nodes_df: pd.DataFrame) -> list[int]:
    gx_context = gx.get_context()
    data_source = gx_context.data_sources.add_pandas("pandas")

    nodes_dataset = data_source.add_dataframe_asset("nodes bronze")
    nodes_batch_def = nodes_dataset.add_batch_definition_whole_dataframe(
        "nodes whole df"
    )
    nodes_batch = nodes_batch_def.get_batch(batch_parameters={"dataframe": nodes_df})

    unique_id = gx.expectations.ExpectColumnValuesToBeUnique(column="id")
    nodes_result = nodes_batch.validate(unique_id)
    return nodes_result.result["partial_unexpected_index_list"]


def validate_edges(edges_df: pd.DataFrame) -> list[int]:
    gx_context = gx.get_context()
    data_source = gx_context.data_sources.add_pandas("pandas")

    edges_dataset = data_source.add_dataframe_asset("edges bronze")
    edges_batch_def = edges_dataset.add_batch_definition_whole_dataframe(
        "edges whole df"
    )
    edges_batch = edges_batch_def.get_batch(batch_parameters={"dataframe": edges_df})

    edges_expectations = gx.ExpectationSuite(name="edges validation")
    edges_expectations.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="dest")
    )
    edges_expectations.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="src")
    )
    edges_result = edges_batch.validate(edges_expectations)
    print(edges_result)
    invalid_indexes = set()
    for res in edges_result.get_failed_validation_results().results:
        invalid_indexes.update(res.result["partial_unexpected_index_list"])

    return list(invalid_indexes)


def clean_nodes(src: str, dest: str):
    nodes_bronze = pd.read_parquet(f"{src}/nodes.parquet")
    invalid_indexes = validate_nodes(nodes_bronze)
    print("nodes:", len(invalid_indexes), "lignes fautives")
    nodes_silver = nodes_bronze.drop(index=invalid_indexes)
    nodes_silver.to_parquet(f"{dest}/nodes.parquet")


def clean_edges(src: str, dest: str):
    edges_bronze = pd.read_parquet(f"{src}/edges.parquet")
    invalid_indexes = validate_edges(edges_bronze)
    print("edges:", len(invalid_indexes), "lignes fautives")
    edges_silver = edges_bronze.drop(index=invalid_indexes)
    edges_silver.to_parquet(f"{dest}/edges.parquet")


arg_parser = argparse.ArgumentParser()
arg_parser.add_argument(
    "--src",  # in is a reserved keyword
    metavar="Source folder",
    type=str,
    required=True,
)
arg_parser.add_argument(
    "--dest",
    metavar="Destination folder",
    type=str,
    required=True,
)
args = arg_parser.parse_args()

clean_nodes(args.src, args.dest)
clean_edges(args.src, args.dest)
