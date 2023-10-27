import os
import glob
from datetime import date
import json

def move_to_persistent(data_dir):
  with open('src/metadata_dictionary.json') as f:
      datasets = json.load(f)
  temporal_dir = os.path.join(data_dir, "landing", "temporal")
  persistent_dir = os.path.join(data_dir, "landing", "persistent")
  for filename in glob.glob(os.path.join(temporal_dir, '*.*')):
    nested_dir = None
    for dataset in datasets:
      if dataset["standard_data_name"] in filename:
        nested_dir = dataset["standard_data_name"]
    
    if nested_dir is None: 
      print(f"Did not recognize dataset {filename}. Add to metadata_dictionary")
      continue
    print(f"Moving {filename}")
    base_name = os.path.basename(filename) # Get last part of the filepath
    filebase, fileextension = base_name.split(".")
    os.rename(os.path.join(filename),
              os.path.join(persistent_dir, nested_dir, f'{filebase}_{date.today()}.{fileextension}'))

