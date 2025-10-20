# Question:
# - avant ou après la transformation en parquet ??
# - Pourquoi gx_checkpoint ??
# - Que fait le script si il detecte des erreurs / données non valables ?


# - Quelles programmes pour neo4j_bulk_import ?

def all_nodes_have_unique_ids(nodes):
    ids = set()
    for node in nodes:
        if node["id"] in ids:
            print(f"Check failed! duplicated id found: ", node)
            exit(1)
        ids.add(node["id"])

def edges_are_not_missing_ids(edges):
    for edge in edges:
        if edge["src"] == None or edge["dest"] == None:
            print("Check failed ! Edge with missing id found: ", edge)