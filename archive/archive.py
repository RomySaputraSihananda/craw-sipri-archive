import os

from json import dumps
from pyquery import PyQuery
from requests import Session, Response
from functools import partial
from time import time

from concurrent.futures import ThreadPoolExecutor

from archive.helpers import Parser, Datetime, logging, counter_time

class Archive():
    def __init__(self):
        self.__BASE_URL: str = 'https://www.sipri.org'
        self.__parser:  Parser = Parser()
        self.__datetime:  Datetime = Datetime()
        self.__requests: Session = Session()

    def __download(self, url: str, output: str):
        if(not os.path.exists(output)):
            os.makedirs(output)

        if(not url): return None, None

        response: Response = self.__requests.get(url)
        
        if(response.status_code != 200): return None, None

        output: str = f'{output}/{url.split("/")[-1].replace("/", "_")[:250]}'

        with open(output, 'wb') as file:
            file.write(response.content) 

        logging.info(f'[pdf] {output}')

        return output.split('/')[-1], output
        
    def __filter_by_book(self, url: str, body_all: PyQuery) -> None:
        response: Response = self.__requests.get(url)

        if(response.status_code != 200): return logging.error(response)
        
        logging.info(url)
        
        body: PyQuery = self.__parser.execute(response.text, 'body')

        output: str = f'data/{self.__parser.execute(body, "h1").text().replace(" ", "_")}'

        url_pdf: str = self.__parser.execute(body, 'ul li:last-child .views-field.views-field-langcode .field-content a').attr('href') 
        file_name, path_data_raw = self.__download((self.__BASE_URL +  url_pdf) if url_pdf else None, output + '/pdf')

        with open(f'{output}/data.json', 'w') as file:
            file.write(dumps({
                "link": url,
                "domain": self.__BASE_URL.split('/')[-1],
                "tag": url.split('/')[2:],
                "category": self.__parser.execute(body_all, '#sipri-2016-breadcrumbs span').text(),
                "sub_category": self.__parser.execute(body_all, 'h1').text(),
                "title": self.__parser.execute(body, 'h1').text(),
                "language": self.__parser.execute(body, '#field-language-display div').remove('label').text(),
                "file_name": file_name, 
                "path_data_raw": path_data_raw, 
                "crawling_time": self.__datetime.now(),
                'crawling_time_epoch': int(time()),
            }, indent=2))

    @counter_time
    def execute(self) -> None:
        response: Response = self.__requests.get('https://www.sipri.org/yearbook/archive')

        if(response.status_code != 200): return logging.error(response)

        body: PyQuery = self.__parser.execute(response.text, 'body')

        urls: list = [self.__BASE_URL + PyQuery(a).attr('href') for a in self.__parser.execute(body, '.views-row .views-field.views-field-title a')]

        with ThreadPoolExecutor() as executor:
            executor.map(partial(self.__filter_by_book, body_all=body), urls)

        executor.shutdown(wait=True)
    
if(__name__ == '__main__'):
    archive: Archive = Archive()
    archive.execute()