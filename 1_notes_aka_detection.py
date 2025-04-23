import pandas as pd
#from matplotlib import pyplot as plt
from tqdm.auto import tqdm 
tqdm.pandas()
import os
import time
import openai
from langchain_core.prompts import PromptTemplate, PromptTemplate
from langchain_openai import ChatOpenAI
from langchain_core.output_parsers import StrOutputParser
from langchain_core.output_parsers import JsonOutputParser
from langchain_core.pydantic_v1 import BaseModel, Field
from typing import Deque, List, Optional, Tuple
from langchain_community.callbacks import get_openai_callback

def has_numbers(x):
    return any([c.isdigit() for c in x])
class ASList(BaseModel):
    ASs: Optional[List[int]] = None#list = Field(description="List of ASs")
def get_ASs(x):
    time.sleep(0.1)
    try:
        # print(x["asn"])
        # print(x["aka"])
        # print(x["notes"])
        out = chain.invoke(input = {"aka":x["aka"], "asn":x["asn"], "notes":x["notes"]})
        #print(x)
        #print(out)
        return out
    except Exception as e:
        #print(e)
        return '[]'

prompt = """
You are a network topology expert who wants to find Autonomous Systems(ASs) that belongs to the same organization by reading the peeringdb information. 

Please inform the ASs that are actively related with the original AS.

The PeeringDB information for the ASN {asn} is:

Notes: {notes}

AKA: {aka}

{format_instructions}
"""

df = pd.read_json("input_files/peeringdb_2_dump_2024_07_24.json")
df_net = pd.DataFrame(df["net"]["data"])[["asn","org_id","website","notes", "aka"]]
df_net_notes = df_net.query("notes != '' or aka != ''")
df_net_notes = df_net_notes[df_net_notes.aka.apply(has_numbers) | df_net_notes.notes.apply(has_numbers)]
parser = JsonOutputParser(pydantic_object=ASList)

prompt = PromptTemplate(
    template=prompt,
    input_variables=["aka", "notes", "asn"],
    partial_variables={"format_instructions": parser.get_format_instructions()},
)

llm = ChatOpenAI(temperature=0, model ="gpt-4o-mini",)
chain = prompt | llm | parser
with get_openai_callback() as cb:
    df_net_notes["llm output"] = df_net_notes.progress_apply(get_ASs, axis = 1)
    print(cb)
df_net_notes.to_hdf("output_files/df_notes_aka.hdf", key="data")