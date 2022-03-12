import pandas as pd


PCworkfile = "PartnerCenter.xlsx"

df_1 = pd.read_excel(PCworkfile)

frames = [df_1]

all_data_df = pd.concat(frames, axis=0)
data_group = all_data_df.groupby(['SubscriptionId']).sum()
final_data = data_group['PostTaxTotal','ResellerCost']
final_data.to_excel('abc.xlsx')