import pickle
from pathlib import Path
import pandas as pd
from PIL import Image
from tqdm.auto import tqdm
import numpy as np
from urllib.request import urlretrieve
import time
import requests
from PIL import Image
from matplotlib import pyplot as plt
from IPython.display import HTML
from io import BytesIO
import base64
import os.path
import io
tqdm.pandas()
from langchain_community.callbacks import get_openai_callback

from langchain_openai import ChatOpenAI

import warnings
warnings.filterwarnings('ignore')

df_favicons = pd.read_feather("output_files/df_favicon.feather")

def convert(x):
    try: 
        out = np.array(Image.open(io.BytesIO(x)).convert("RGBA")).flatten()
        if len(out) != 1024:
            out = None
    except:
        out = None
    return out

def convert_gray(x):
    try: 
        out = np.array(Image.open(io.BytesIO(x)).convert("RGBA")).flatten()
        if len(out) != 1024:
            out = None
    except:
        out = None
    return out

def image_formatter(im):
    return f'<img src="data:image/jpeg;base64,{image_base64(im)}">'
def get_img(x):
    return Image.open(io.BytesIO(x)).convert("RGBA")
def image_base64(im):
    if isinstance(im, str):
        im = get_thumbnail(im)
    with BytesIO() as buffer:
        im.save(buffer, 'png')
        return base64.b64encode(buffer.getvalue()).decode("utf-8")

df_favicons["favicon_img"] = df_favicons.favicon.apply(get_img)

favicons = df_favicons.favicon.value_counts()[df_favicons.favicon.value_counts() >2].to_frame().reset_index()
idx = range(len(favicons))
favicons["favicon_img"] = favicons.favicon.apply(get_img)

df_favicons = df_favicons[df_favicons.favicon.isin(favicons.favicon.iloc[1:])]

import os
import langchain

from langchain_core.messages import HumanMessage
from langchain_openai import ChatOpenAI

llm = ChatOpenAI(model = "gpt-4o-mini", temperature = 0)

import httpx

def get_description(x):
    image_data = base64.b64encode(x["favicon"]).decode("utf-8")
    message = HumanMessage(
        content=[
            {"type": "text", "text": f"Accediendo a estas urls {x['final_url']} se obtuvo el favicon adjunto. En caso de que sea una compañía de telecomunicaciones. Cuál es el nombre de la compañía?.Si es una subsidiaria, dar el nombre de la compañia madre. Si no es una compañía de telecomunicaciones, es una tecnología de hosting? Responder solo el nombre de la companía o la tecnología. Si no es ninguna de las anteriores responder 'no sé'."},
            {
                "type": "image_url",
                "image_url": {"url": f"data:image/jpeg;base64,{image_data}"},
            },
        ],
    )
    return llm.invoke([message])

df_to_llm = df_favicons.groupby("favicon").final_url.sum().to_frame().reset_index()

with get_openai_callback() as cb:
    df_to_llm["desc"] = df_to_llm.progress_apply(get_description, axis = 1)
    print(cb)

df_to_llm["favicon_img"] = df_to_llm["favicon"].apply(get_img)

df_to_llm["content"] = df_to_llm.desc.apply(lambda x:x.content)

df_favicons.merge(df_to_llm[["favicon","content"]], on="favicon", how="left").to_hdf("output_files/df_favicon_desc.hdf", key="data")