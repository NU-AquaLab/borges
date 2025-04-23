import pandas as pd

df = pd.read_csv("input_files/20240701.as-org2info.txt", skiprows= 95315, sep="|").rename(columns={"# format:aut":"ASN"})

df["ASN"] = df.ASN.apply(lambda x:[x])

df.groupby("org_id").ASN.sum().to_frame().to_feather("df_whois.feather")