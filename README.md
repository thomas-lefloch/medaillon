# medaillon

Construction d'une architecture en medaillon (Bronze -> Silver -> Gold)

## Execution

Lancer tout les applications (airflow, neo4j, api) avec docker compose:
```sh
docker compose up
# methode alternative
make up
```

Pour générer et insérer les données dans neo4j on peut soit:
- Lancer le dag airflow `full_process` depuis l'interface web d'airflow (http://localhost:8080)
- éxecuter la commande  `make e2e` pour construire depuis le pc actuel.

## Infrastructure

- Airflow (http://localhost:8080) (Logins [ici](configs/passwords.json.generated) après avoir lancer airflow)
- Neo4j http://localhost:7474 pour l'interface web et http://localhost:7687 pour la base de donnée
- API http://localhost:8000

Les données sont écritent dans le dossier `data`selon leur étape

## routes d'API
- /health renvoie {status:"healthy"}
- /entity/{id} renvoie le Node correspondant à l'id
- /query/cypher éxécute une requête cypher et renvoie le résultat