from .Datetime import Datetime
from .Parser import Parser

import logging
from logging import handlers

logging.basicConfig(level=logging.INFO, format='%(asctime)s [ %(levelname)s ] :: %(message)s', datefmt="%Y-%m-%dT%H:%M:%S", handlers=[
    handlers.RotatingFileHandler('debug.log'),  
    logging.StreamHandler()  
])

from time import perf_counter

def counter_time(func):
    def counter(self):
        start: float = perf_counter()
        logging.info('start crawling')
        func(self)
        logging.info(f'task completed in {perf_counter() - start} seconds')
    
    return counter