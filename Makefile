clean:
	rm -f data/raw/*.csv
	rm -f data/bronze/*.parquet
	rm -r -f data/silver/edges
	rm -f data/silver/*.parquet
	rm -f data/gold/*.csv
	rm -f config/*.cfg config/*.generated
	docker compose down -v

seed:
	python ./scripts/generate_sample_data.py --out data/raw --nodes 1000000 --edges 5000000

bronze:
	python ./scripts/to_parquet.py --src data/raw --dest data/bronze

silver:
	python ./quality/gx_checkpoint.py --src data/bronze --dest data/silver
	python ./scripts/partition_edges.py --edges data/silver/edges.parquet --dest data/silver/edges --partitions 8
	rm data/silver/edges.parquet

gold:
	sh ./scripts/neo4j_bulk_import.sh


e2e: clean seed bronze silver gold

	
up:
	docker compose up -d

stop:
	docker compose stop

down:
	docker compose down