clean:
	rm data/raw/edges.csv data/raw/nodes.csv

seed:
	python ./scripts/generate_sample_data.py --out data/raw --nodes 1000000 --edges 5000000