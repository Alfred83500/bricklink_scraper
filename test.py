import pandas as pd
 
chunk = pd.read_csv('data/sets_scraped.csv')

for index, rows in chunk.iterrows():
    print(index)
