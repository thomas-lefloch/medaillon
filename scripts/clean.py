import os
import glob
import shutil


def clean():
    for file in glob.glob("data/raw/*.csv"):
        os.remove(file)

    for file in glob.glob("data/bronze/*.parquet"):
        os.remove(file)

    shutil.rmtree("data/silver/edges")
    for file in glob.glob("data/silver/*.parquet"):
        os.remove(file)

    for file in glob.glob("data/gold/*.csv"):
        os.remove(file)
