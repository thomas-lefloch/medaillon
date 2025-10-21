# Question:
# - avant ou après la transformation en parquet ?? apres (début silver)
# - Pourquoi gx_checkpoint ?? Great Expectation (biblio)
# - Que fait le script si il detecte des erreurs / données non valables ?
# suppression

# - Quelles programmes pour neo4j_bulk_import ?
# requete cypher en python dans .sh

import great_expectations as gx
import pandas as pd

nodes_bronze = pd.read_parquet("data/bronze/nodes.parquet")
edges_bronze = pd.read_parquet("data/bronze/edges.parquet")

gx_context = gx.get_context()
data_source = gx_context.data_sources.add_pandas("pandas")

nodes_dataset = data_source.add_dataframe_asset("nodes bronze")
nodes_batch_def = nodes_dataset.add_batch_definition_whole_dataframe("nodes whole data")
nodes_batch = nodes_batch_def.get_batch(batch_parameters={"dataframe": nodes_bronze})

unique_id = gx.expectations.ExpectColumnValuesToBeUnique(column="id")
nodes_result = nodes_batch.validate(unique_id)
print(nodes_result)

edges_dataset = data_source.add_dataframe_asset("edges bronze")
edges_batch_def = edges_dataset.add_batch_definition_whole_dataframe("edges whole data")
edges_batch = edges_batch_def.get_batch(batch_parameters={"dataframe": edges_bronze})

edges_expectations = gx.ExpectationSuite(name="edges validation")
edges_expectations.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(column="dest")
)
edges_expectations.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(column="src")
)
edges_result = edges_batch.validate(edges_expectations)
print(nodes_result, edges_result)
