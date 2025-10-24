from fastapi import FastAPI
from neo4j import GraphDatabase
import os

app = FastAPI()
neo4j_uri = os.environ["NEO4J_URI"]


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "healthy"}


@app.get("/entity/{id}")
def get_entity_by_id(id: int):
    with GraphDatabase.driver(neo4j_uri) as driver:
        res = driver.execute_query("MATCH (n:Node{id:$id}) RETURN n", {"id": id})
        return res.records


@app.post("/query/cypher")
def make_cypher_request(query: str):
    with GraphDatabase.driver(neo4j_uri) as driver:
        res = driver.execute_query(query)
        return res.records
