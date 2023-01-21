import pandas as pd
import numpy
import requests


from urllib.request import Request, urlopen
from google.cloud import storage


def get_sets():
    req_set = Request('https://rebrickable.com/media/downloads/sets.csv.gz')
    req_set.add_header('User-Agent', 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:77.0) Gecko/20100101 Firefox/77.0')
    content_set = urlopen(req_set)
    sets = pd.read_csv(content_set, compression='gzip')

    return sets


def get_themes():
    req_themes = Request('https://rebrickable.com/media/downloads/themes.csv.gz')
    req_themes.add_header('User-Agent', 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:77.0) Gecko/20100101 Firefox/77.0')
    content_themes = urlopen(req_themes)
    themes = pd.read_csv(content_themes, compression='gzip')
    themes = themes.rename(columns={'id': 'theme_id'})
    themes = themes.drop(columns='parent_id')
    return themes


def merge_sets_for_scraping(sets, themes):
    sets_for_scrapping = sets.merge(themes, on='theme_id')
    sets_for_scrapping = sets_for_scrapping.rename(columns={'name_y': 'theme_name'})
    sets_for_scrapping = sets_for_scrapping.rename(columns={'name_x': 'set_name'})
    return sets_for_scrapping


def save_to_gcp(sets_for_scrapping):
    project_id = "silver-nova-360910"
    bucket = 'bucket-lego'
    export_bucket = storage.Client(project=project_id).get_bucket(bucket)
    export_bucket.blob('source/sets_for_scrapping.csv').upload_from_string(sets_for_scrapping.to_csv(), 'text/csv')


def main():
    sets = get_sets()
    themes = get_themes()
    sets_merged = merge_sets_for_scraping(sets, themes)
    save_to_gcp(sets_merged)


if __name__ == "__main__":
    main()
