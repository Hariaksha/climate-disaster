import requests, datetime, openpyxl, csv, time
from bs4 import BeautifulSoup

def get_request(link):
    while True:  
        try:
            x = requests.get(link, timeout=30)
            ans = BeautifulSoup(x.content, 'html.parser')
            break
        except:
            print('Connection lost. Retrying in 10 seconds.')
            time.sleep(10)
    return ans

# https://developers.google.com/custom-search/v1/overview

def main():
    start = last = datetime.datetime.now()
    filename = open('events-US-1980-2024-Q4.csv') 
    file = csv.DictReader(filename)
    workbook = openpyxl.load_workbook('')
    ws = workbook.active
    