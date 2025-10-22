from airflow.sdk import dag, task


@task
def seed():
    # generate data
    pass

@task
def bronze():
    # transform into parquet
    pass

@task
def silver():
    # clean and partition data
    pass

@task
def gold():
    # import into neo4j
    pass

@dag(dag_id="etl")
def etl():
    seed() >> bronze() >> silver() >> gold()
    
etl()