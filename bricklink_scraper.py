import pandas as pd
import numpy as np
import time
import requests
from bs4 import BeautifulSoup as bs
from pandas.errors import ParserError
from google.cloud import storage
from selenium import webdriver  # for webdriver

from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver import DesiredCapabilities
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.options import Options
import datetime

project_id = "silver-nova-360910"
bucket = 'bucket-lego'
export_bucket = storage.Client(project=project_id).get_bucket(bucket)
print(export_bucket)
date = datetime.datetime.now()
year_data = date.year
if date.month < 10:
    month_data = f'{year_data}-M0{date.month}'
else:
    month_data = f'{year_data}-M{date.month}'


def create_log():
    with export_bucket.blob(f'output/log.txt').open('w') as f:
        f.write(f"script launched: {date}")

    return


def to_chunk():
    # project_id = "silver-nova-360910"
    # bucket = storage.Client(project=project_id).get_bucket('bucket-lego')
    # sets = pd.read_csv('gs://bucket-lego/output/brickset/brickset_scrapped.csv')
    for i, chunk in enumerate(pd.read_csv('gs://bucket-lego/output/brickset/brickset_scrapped.csv', chunksize=10)):
        chunk.to_csv(f'gs://bucket-lego/source/bricklink/tmp/bricklink_part_{i + 1}.csv', index=False)


# def get_date(sets):
#     sets['year_data'] = year_data
#      sets['month_data'] = month_data
#      sets_date = sets


def scrapper():
    
    for i in range(13, 25):

        PATH = 'C:\Program Files (x86)\chromedriver.exe'
        options = Options()
        options.headless = True
        options.add_argument("--incognito")
        options.add_argument(
            '--Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36')
        # options.add_argument('--Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36')
        chunk_size = 499

        price_new_usa = []
        price_used_usa = []
        lots_new_usa = []
        lots_used_usa = []
        qty_new_usa = []
        qty_used_usa = []
        price_new_eur = []
        price_used_eur = []
        lots_new_eur = []
        lots_used_eur = []
        qty_new_eur = []
        qty_used_eur = []

        with export_bucket.blob(f'output/log.txt').open('w') as f:
            f.write(f'chunk number {i + 1} started at: {datetime.datetime.now()} ')

        chunk = pd.read_csv(f'gs://bucket-lego/source/bricklink/tmp/bricklink_part_{i + 1}.csv')
        chunk['year_data'] = year_data
        chunk['month_data'] = month_data

        for index, row in chunk.iterrows():
            driver = webdriver.Chrome(options=options, executable_path=PATH)

            try:
                driver.get("https://www.bricklink.com/v2/catalog/catalogitem.page?S=" + str(row['set_num']) + "#T=P")
                button = driver.find_element("xpath", "//div[@id='js-btn-save']//child::button").click()
                checkbox_currency = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.ID, "_idchkPGGroupByCurrency")))
                checkbox_incomplete = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.ID, "_idchkPGExcludeIncomplete")))
                if not checkbox_currency.is_selected():
                    checkbox_currency.click()
                if not checkbox_incomplete.is_selected():
                    checkbox_incomplete.click()
            except Exception as e:
                print(str(row['set_num']) + '- Echec' + str(e))

            try:
                price_new_usa.append(WebDriverWait(driver, 5).until(EC.presence_of_element_located(
                    (By.XPATH, "//*[@id='_idPGContents']/table/tbody/tr[4]/td[3]/table/tbody/tr[3]/td[2]/b"))).text)
                print(WebDriverWait(driver, 5).until(EC.presence_of_element_located(
                    (By.XPATH, "//*[@id='_idPGContents']/table/tbody/tr[4]/td[3]/table/tbody/tr[3]/td[2]/b"))).text)

            except Exception as e:
                print(str(row['set_num']) + '- Echec' + str(e))
                price_new_usa.append('n/a')

            try:
                price_used_usa.append(WebDriverWait(driver, 5).until(EC.presence_of_element_located(
                    (By.XPATH, "//*[@id='_idPGContents']/table/tbody/tr[4]/td[4]/table/tbody/tr[3]/td[2]/b"))).text)
                print(WebDriverWait(driver, 5).until(EC.presence_of_element_located(
                    (By.XPATH, "//*[@id='_idPGContents']/table/tbody/tr[4]/td[4]/table/tbody/tr[3]/td[2]/b"))).text)
            except Exception as e:
                print(str(row['set_num']) + '- Echec' + str(e))
                price_used_usa.append('n/a')

            try:
                lots_new_usa.append(WebDriverWait(driver, 5).until(EC.presence_of_element_located(
                    (By.XPATH, "//*[@id='_idPGContents']/table/tbody/tr[4]/td[3]/table/tbody/tr[1]/td[2]/b"))).text)
                print(WebDriverWait(driver, 5).until(EC.presence_of_element_located(
                    (By.XPATH, "//*[@id='_idPGContents']/table/tbody/tr[4]/td[3]/table/tbody/tr[1]/td[2]/b"))).text)
            except Exception as e:
                print(str(row['set_num']) + '- Echec' + str(e))
                lots_new_usa.append('n/a')

            try:
                lots_used_usa.append(WebDriverWait(driver, 5).until(EC.presence_of_element_located(
                    (By.XPATH, "//*[@id='_idPGContents']/table/tbody/tr[4]/td[4]/table/tbody/tr[1]/td[2]/b"))).text)
            except Exception as e:
                print(str(row['set_num']) + '- Echec' + str(e))
                lots_used_usa.append('n/a')

            try:
                qty_used_usa.append(WebDriverWait(driver, 5).until(EC.presence_of_element_located(
                    (By.XPATH, '//*[@id="_idPGContents"]/table/tbody/tr[4]/td[3]/table/tbody/tr[2]/td[2]/b'))).text)
            except Exception as e:
                print(str(row['set_num']) + '- Echec' + str(e))
                qty_used_usa.append('n/a')

            try:
                qty_new_usa.append(WebDriverWait(driver, 5).until(EC.presence_of_element_located(
                    (By.XPATH, "//*[@id='_idPGContents']/table/tbody/tr[4]/td[4]/table/tbody/tr[2]/td[2]/b"))).text)
            except Exception as e:
                print(str(row['set_num']) + '- Echec' + str(e))
                qty_new_usa.append('n/a')

            try:
                price_new_eur.append(WebDriverWait(driver, 5).until(EC.presence_of_element_located(
                    (By.XPATH, "//*[@id='_idPGContents']/table/tbody/tr[7]/td[3]/table/tbody/tr[3]/td[2]/b"))).text)
                print(WebDriverWait(driver, 5).until(EC.presence_of_element_located(
                    (By.XPATH, "//*[@id='_idPGContents']/table/tbody/tr[7]/td[3]/table/tbody/tr[3]/td[2]/b"))).text)
            except Exception as e:
                print(str(row['set_num']) + '- Echec' + str(e))
                price_new_eur.append('n/a')

            try:
                price_used_eur.append(WebDriverWait(driver, 5).until(EC.presence_of_element_located(
                    (By.XPATH, "//*[@id='_idPGContents']/table/tbody/tr[7]/td[4]/table/tbody/tr[3]/td[2]/b"))).text)
                print(WebDriverWait(driver, 5).until(EC.presence_of_element_located(
                    (By.XPATH, "//*[@id='_idPGContents']/table/tbody/tr[7]/td[4]/table/tbody/tr[3]/td[2]/b"))).text)
            except Exception as e:
                print(str(row['set_num']) + '- Echec' + str(e))
                price_used_eur.append('n/a')

            try:
                lots_new_eur.append(WebDriverWait(driver, 5).until(EC.presence_of_element_located(
                    (By.XPATH, "//*[@id='_idPGContents']/table/tbody/tr[7]/td[3]/table/tbody/tr[1]/td[2]/b"))).text)
                print(WebDriverWait(driver, 5).until(EC.presence_of_element_located(
                    (By.XPATH, "//*[@id='_idPGContents']/table/tbody/tr[7]/td[3]/table/tbody/tr[1]/td[2]/b"))).text)
            except Exception as e:
                print(str(row['set_num']) + '- Echec' + str(e))
                lots_new_eur.append('n/a')

            try:
                lots_used_eur.append(WebDriverWait(driver, 5).until(EC.presence_of_element_located(
                    (By.XPATH, "//*[@id='_idPGContents']/table/tbody/tr[7]/td[4]/table/tbody/tr[1]/td[2]/b"))).text)
            except Exception as e:
                print(str(row['set_num']) + '- Echec' + str(e))
                lots_used_eur.append('n/a')
            try:
                qty_new_eur.append(WebDriverWait(driver, 5).until(EC.presence_of_element_located(
                    (By.XPATH, '//*[@id="_idPGContents"]/table/tbody/tr[7]/td[3]/table/tbody/tr[2]/td[2]/b'))).text)
            except Exception as e:
                print(str(row['set_num']) + '- Echec' + str(e))
                qty_new_eur.append('n/a')
            try:
                qty_used_eur.append(WebDriverWait(driver, 5).until(EC.presence_of_element_located(
                    (By.XPATH, "//*[@id='_idPGContents']/table/tbody/tr[7]/td[4]/table/tbody/tr[2]/td[2]/b"))).text)
            except Exception as e:
                print(str(row['set_num']) + '- Echec' + str(e))
                qty_used_eur.append('n/a')

            driver.quit()

        chunk_scraped = chunk
        chunk_scraped['price_new_usa'] = price_new_usa
        chunk_scraped['price_used_usa'] = price_used_usa
        chunk_scraped['lots_new_usa'] = lots_new_usa
        chunk_scraped['lots_used_usa'] = lots_used_usa
        chunk_scraped['qty_new_usa'] = qty_new_usa
        chunk_scraped['qty_used_usa'] = qty_used_usa
        chunk_scraped['price_new_eur'] = price_new_eur
        chunk_scraped['price_used_eur'] = price_used_eur
        chunk_scraped['lots_new_eur'] = lots_new_eur
        chunk_scraped['lots_used_eur'] = lots_used_eur
        chunk_scraped['qty_new_eur'] = qty_new_eur
        chunk_scraped['qty_used_eur'] = qty_used_eur

        export_bucket.blob(f'output/bricklink/tmp/bricklink_scraped_part_{i + 1}_{month_data}.csv').upload_from_string(
            chunk_scraped.to_csv(), 'text/csv')

        with export_bucket.blob(f'output/log.txt').open('w') as f:
            f.write(f'chunk number {i + 1} finished at: {datetime.datetime.now()}')
        i += 1
        time.sleep(18000)

    with export_bucket.blob(f'output/log.txt').open('w') as f:
        f.write(f'script finished at: {datetime.datetime.now()}')


def main():
    # create_log()
    # to_chunk()
    scrapper()


if __name__ == "__main__":
    main()
