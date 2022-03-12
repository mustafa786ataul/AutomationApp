import pandas as pd
import numpy as np

workfile1 = "finalFile.xlsx"
workfile2 = "CMPFile.xlsx"

df_1 = pd.read_excel(workfile1)
df_2 = pd.read_excel(workfile2)

df_1.reset_index(drop=True)
df_2.reset_index(drop=True)
#print(df_1)
#print(df_2)

Left_join = pd.merge(df_1,df_2[['SubscriptionId','ResellerCompanyName']], on = "SubscriptionId", how= 'left')

Left_join.to_excel("abc123.xlsx",index=False)

#print(Left_join)

