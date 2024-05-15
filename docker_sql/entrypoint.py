import subprocess
import sys

args = sys.argv[1:]

subprocess.run(["python", "ingest_data.py"] + args)

subprocess.run(["python", "add_taxi_zone_lookup_file.py"])