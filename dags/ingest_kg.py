from airflow.sdk import dag, task
from scripts.generate_sample_data import generate_sample_data
from scripts.partition_edges import partition_edges
from scripts.to_parquet import to_parquet
from quality.gx_checkpoint import gx_checkpoint
from scripts.neo4j_bulk_import import to_csv, import_into_neo4j
import os


@task
def seed():
    generate_sample_data(1_000_000, 5_000_000, "data/raw")


@task
def bronze():
    to_parquet("data/raw", "data/bronze")


@task
def silver():
    gx_checkpoint("data/bronze", "data/silver")
    partition_edges("data/silver/edges.parquet", 8, "data/silver/edges")
    os.remove("data/silver/edges.parquet")


@task
def neo4j_import():
    to_csv()
    import_into_neo4j()


@dag(dag_id="full_process")
def full_process():
    seed() >> bronze() >> silver() >> neo4j_import()


full_process()
