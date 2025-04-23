import pickle
from pathlib import Path
import pandas as pd
from PIL import Image
from tqdm import tqdm
import numpy as np
import os.path

from urllib.request import urlretrieve
import time
import requests

df = pd.read_json("input_files/peeringdb_2_dump_2024_07_24.json")
df_net = pd.DataFrame(df["net"]["data"])[["asn","org_id","website","notes"]]

df_net_not_empty = df_net.query("website != ''")

df_webs = df_net_not_empty.website.value_counts().reset_index()

html_files = list(Path("raw_htmls_2024/").glob("http*"))

data = list()
for file in tqdm(html_files):
    with open(file, "rb") as f:
        html = pickle.load(f)
    data.append(html)

df = pd.DataFrame(data, columns=["orig_url", "redirects", "final_url", "raw_html"])

def get_favicon_url(x):
    return f"https://t3.gstatic.com/faviconV2?client=SOCIAL&type=FAVICON&fallback_opts=TYPE,SIZE,URL&url={x}&size=16"


def get_favicon(url):
    headers = {
        'User-Agent': 'My User Agent 1.0',
    #    'From': 'youremail@domain.example'  # This is another valid field
    }
    if not os.path.isfile("favicons_2024/"+url.replace("/","_")):
        try:
            favicon_url = get_favicon_url(url)
            resp = requests.get(favicon_url, headers=headers)
            result = (url,resp.content)
            with open("favicons_2024/"+url.replace("/","_"), "wb") as f:
                pickle.dump(result,f)
            return result
        except Exception as e:
            return None,None
    else:
        return None,None

df["favicon_url"] = df.final_url.apply(get_favicon_url)

urls = df.final_url.to_list()

from multiprocessing import Pool

NUM_OF_PROCS = 100
pool = Pool(NUM_OF_PROCS)
results = pool.map(get_favicon, urls)

df_favicon = pd.DataFrame(results, columns = ["final_url", "favicon"])

df_favicon = df_favicon.groupby("final_url").head(1)

df_favicon.to_feather("output_files/df_favicon.feather")
