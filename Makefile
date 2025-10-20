clean:
	rm data/raw/edges.csv data/raw/nodes.csv

seed:
	python ./scripts/generate_sample_data.py --out data/raw --nodes 1000000 --edges 5000000

bronze:
	python ./scripts/to_parquet.py --src data/raw --dest data/bronze

silver:
	python ./scripts/partition_edges.py --src data/bronze --dest data/silver --partitions 8