import pandas as pd
from pathlib import Path
import json
import pickle
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import os

def get_url(url):
    headers = {
        'User-Agent': 'My User Agent 1.0',
    #    'From': 'youremail@domain.example'  # This is another valid field
    }
    if not os.path.isfile("raw_htmls_2024/"+url.replace("/","_")):
        try:
            resp = requests.get(url, headers=headers)
            redirects = [hist.url for hist in resp.history]
            final_url = resp.url
            result = (url, redirects, final_url, resp.text)
            with open("raw_htmls_2024/"+url.replace("/","_"), "wb") as f:
                pickle.dump(result,f)
            return result
        except Exception as e:
            return (None,None,None,None)
    else:
        return (None,None,None,None)

args = []
pdb_filename = "input_files/peeringdb_2_dump_2024_07_24.json"
with open(pdb_filename, "r") as f:
    pdb = json.load(f)

for net in pdb["net"]["data"]:
    if len(net["website"]) > 0:
        args.append((net["website"], net["asn"],net["org_id"]))

df_peeringdb = pd.DataFrame(args, columns = ["url", "asn", "org_id"])

urls = df_peeringdb.url.value_counts().index.tolist()

from multiprocessing import Pool

def my_function(url):
    result = get_url(url)
    return result

NUM_OF_PROCS = 500
pool = Pool(NUM_OF_PROCS)
results = pool.map(my_function, urls)