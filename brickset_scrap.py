import pandas as pd
from IPython.display import display
import numpy as np
from datetime import datetime
import requests
from bs4 import BeautifulSoup as bs
from pandas.errors import ParserError
from google.cloud import storage


def read_csv():
    project_id = "silver-nova-360910"
    bucket = storage.Client(project=project_id).get_bucket('bucket-lego')
    blob_2 = 'source/sets_for_scrapping.csv'
    sets = pd.read_csv('gs://bucket-lego/source/sets_for_scrapping.csv')
    return sets


def to_reduce_step1(sets):
    numeric_value = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']
    sets_1 = sets
    sets_1 = sets_1.drop(sets_1[sets_1.year < 1996].index)
    sets_1 = sets_1.drop(sets_1[sets_1.num_parts <= 2].index)
    sets_1 = sets_1.drop(sets_1.loc[(sets_1['theme_name'] == 'Books')].index)
    sets_1 = sets_1.drop(sets_1[sets_1.set_num.str[:3] == 'TRU'].index)
    sets_1 = sets_1.drop(sets_1[~sets_1['set_num'].str[:1].isin(numeric_value)].index)
    return sets_1


def scrapper(sets_1):
    minifig = []
    launchexit = []
    rrp = []
    subthemes = []
    availability = []

    for index, row in sets_1.iterrows():

        URL = "https://brickset.com/sets/" + str(row['set_num'])
        response = requests.get(URL)
        html = response.content
        soup = bs(html, "html.parser")

        try:
            minifig.append(str(soup.find('dt', text='Minifigs').findNext('dd').text.strip()))
            print(str(soup.find('dt', text='Minifigs').findNext('dd').text.strip()))

        except(AttributeError):
            minifig.append('n/a')
            print('n/a')

        try:
            launchexit.append(
                str((soup.find('div', class_='text').find('dt', text='Launch/exit').find_next_sibling('dd').text)))
            print((soup.find('div', class_='text').find('dt', text='Launch/exit').find_next_sibling('dd').text))
        except(AttributeError):
            launchexit.append('n/a')
            print('n/a')

        try:
            rrp.append(str((soup.find('div', class_='text').find('dt', text='RRP').find_next_sibling('dd').text)))
            print((soup.find('div', class_='text').find('dt', text='RRP').find_next_sibling('dd').text))
        except:
            rrp.append('n/a')
            print('n/a')
        try:
            subthemes.append(
                str((soup.find('div', class_='text').find('dt', text='Subtheme').find_next_sibling('dd').text)))
            print((soup.find('div', class_='text').find('dt', text='Subtheme').find_next_sibling('dd').text))
        except:
            subthemes.append('n/a')
            print('n/a')
        try:
            availability.append(
                str((soup.find('div', class_='text').find('dt', text='Availability').find_next_sibling('dd').text)))
            print((soup.find('div', class_='text').find('dt', text='Availability').find_next_sibling('dd').text))
        except:
            availability.append('n/a')
            print('n/a')

    sets_scrapped = sets_1
    sets_scrapped['DateEntreSortie'] = launchexit
    sets_scrapped['RRP'] = rrp
    sets_scrapped['minifig'] = minifig
    sets_scrapped['subtheme'] = subthemes
    sets_scrapped['availability'] = availability

    return sets_scrapped


def to_clean(sets_scrapped):
    sets_scrapped['Date_Ouv'] = sets_scrapped['DateEntreSortie'].str[:11]
    sets_scrapped['Date_Clot'] = sets_scrapped['DateEntreSortie'].str[14:]
    sets_scrapped['Date_Clot'].replace('{t.b.a}', 'n/a', inplace=True)
    sets_scrapped["DateEntreSortie"].fillna("n/a", inplace=True)
    sets_scrapped["Date_Ouv"].fillna("n/a", inplace=True)
    sets_scrapped["Date_Clot"].fillna("n/a", inplace=True)
    sets_scrapped["minifig"].fillna("n/a", inplace=True)
    Date_Ouv2 = []
    Date_Clot2 = []

    for index, row in sets_scrapped.iterrows():
        if row['Date_Ouv'] != 'n/a':
            Date_Ouv2.append((pd.to_datetime(row['Date_Ouv']).date()))
        else:
            Date_Ouv2.append(row['Date_Ouv'])

        if row['Date_Clot'] != 'n/a':
            Date_Clot2.append((pd.to_datetime(row['Date_Clot']).date()))
        else:
            Date_Clot2.append(row['Date_Clot'])

    sets_scrapped['Date_Ouv2'] = Date_Ouv2
    sets_scrapped['Date_Clot2'] = Date_Clot2
    del sets_scrapped['Date_Ouv']
    del sets_scrapped['Date_Clot']
    sets_scrapped = sets_scrapped.rename(columns={"Date_Ouv2": "Date_ouverture", "Date_Clot2": "Date_cloture"})
    sets_scrapped['minifig_non_excl'] = sets_scrapped['minifig'].str[:2]
    sets_scrapped['minifig_excl'] = sets_scrapped['minifig'].str[2:6]
    sets_scrapped['minifig_non_excl'] = sets_scrapped['minifig_non_excl'].str.extract('(\d+)')
    sets_scrapped['minifig_excl'] = sets_scrapped['minifig_excl'].str.extract('(\d+)')
    sets_scrapped['minifig_non_excl'] = sets_scrapped['minifig_non_excl'].str.strip()
    sets_scrapped['minifig_excl'] = sets_scrapped['minifig_excl'].str.strip()
    sets_scrapped["minifig_excl"].fillna("n/a", inplace=True)
    sets_scrapped["minifig_non_excl"].fillna("n/a", inplace=True)
    sets_scrapped.minifig_non_excl = sets_scrapped.minifig_non_excl.replace('n/a', 0)
    sets_scrapped.minifig_excl = sets_scrapped.minifig_excl.replace('n/a', 0)
    sets_scrapped.minifig_excl = sets_scrapped.minifig_excl.astype(int)
    sets_scrapped.minifig_non_excl = sets_scrapped.minifig_non_excl.astype(int)
    sets_scrapped = sets_scrapped.replace('n/a', np.nan)
    sets_scrapped.minifig_non_excl = np.subtract(sets_scrapped.minifig_non_excl, sets_scrapped.minifig_excl)
    brickset_scrapped = sets_scrapped
    return brickset_scrapped


def to_reduce(brickset_scrapped):
    brickset_scrapped = brickset_scrapped
    sets_reduced = brickset_scrapped
    sets_reduced = sets_reduced.drop(
        sets_reduced.loc[(sets_reduced['num_parts'] == 3) & (sets_reduced['minifig_non_excl'] > 0)].index)

    return sets_reduced





def main():
    sets = read_csv()
    sets_1 = to_reduce_step1(sets)
    scrapped = scrapper(sets_1)
    sets_cleaned = to_clean(scrapped)
    brickset_scrapped = to_reduce(sets_cleaned)
    save_to_gcp(brickset_scrapped)


if __name__ == "__main__":
    main()
