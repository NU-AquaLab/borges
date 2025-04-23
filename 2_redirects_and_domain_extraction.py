from pathlib import Path
from tqdm import tqdm
import json
#from html_similarity import style_similarity, structural_similarity, similarity
import pandas as pd
import numpy as np
from IPython.core.display import HTML
from bs4 import BeautifulSoup
import pickle
import tldextract

html_files = list(Path("raw_htmls_2024/").glob("http*"))
data = list()
for file in tqdm(html_files):
    with open(file, "rb") as f:
        html = pickle.load(f)
    data.append(html)
args = []
pdb_filename = "input_files/peeringdb_2_dump_2024_07_24.json"
with open(pdb_filename, "r") as f:
    pdb = json.load(f)

for net in pdb["net"]["data"]:
    if len(net["website"]) > 0:
        args.append((net["website"], net["asn"],net["org_id"]))
df_peeringdb = pd.DataFrame(args, columns = ["url", "asn", "org_id"])
df_peeringdb["asn"] = df_peeringdb.asn.apply(lambda x:[x])
df = pd.DataFrame(data, columns=["orig_url", "redirects", "final_url", "raw_html"])
df_merged = df_peeringdb.merge(right=df, right_on="orig_url", left_on = "url")
df_groups_final_url = df_merged.groupby("final_url").asn.sum().to_frame()
df_groups_final_url.to_feather("output_files/df_redirects.feather")
df_groups_final_url["len"]  = df_groups_final_url.asn.apply(lambda x:len(x))
df_groups_final_url.reset_index(inplace=True)
df_groups_final_url["domain"] = df_groups_final_url.final_url.apply(lambda x:tldextract.extract(x).domain)
df_groups_final_url.groupby("domain").asn.sum().to_frame().to_feather("output_files/df_redirects_domain.feather")