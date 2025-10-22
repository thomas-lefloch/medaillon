from fastapi import FastAPI
import neo4j

app = FastAPI()


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "healthy" }

@app.get("/entity/{id}")
def get_entity_by_id(id: int):
    pass

@app.post("/query/cypher")
def make_cypher_request():
    pass
