from urllib.parse import quote_plus
from math import ceil
import os
import logging
import requests

BASE_URL = 'https://dictionaryapi.com/api/v3/references/'

DB_PRODUCER_NUM = 5
WEB_PRODUCER_NUM = 5
PARSER_CONSUMER = 10
WRITER_CONSUMER = 5

KEY_FILE = 'learners.key'
KEY = None
INVALID_KEY_TEXT = 'Invalid API key. Not subscribed for this reference.'

def get_key_interactive():
    print("Please get MERRIAM-WEBSTER'S learners api key from https://www.dictionaryapi.com/")
    print("Input your key, Ctrl+D to exit...")
    try:
        global KEY
        while True:
            KEY = input('>>>')
            url = get_query_url('test')
            resp = requests.get(url)
            if resp.status_code == 200 and resp.text != INVALID_KEY_TEXT:
                print(f"Valid key {KEY}, write to '{KEY_FILE}'")
                with open(KEY_FILE, 'w') as fid:
                    fid.write(KEY)
                break
            else:
                print(f"Invalid key {KEY}, check again!")
    except EOFError:
        exit(0)
    except Exception as err:
        print(f'Error {type(err).__name__}')

def read_key():
    if not os.path.exists(KEY_FILE):
        logging.fatal("MERRIAM-WEBSTER'S learners api key is't exists.")
        get_key_interactive()
    else:
        with open(KEY_FILE, 'r') as fid:
            global KEY
            KEY = fid.read().strip()

def split_collection(collection, chunk_num):
    length = len(collection)
    chunk_size = ceil(length / chunk_num)
    split_list = [[] for _ in range(chunk_num)]

    for idx, value in enumerate(collection):
        chenk_idx = idx // chunk_size
        split_list[chenk_idx].append(value)

    filter_list = [coll for coll in split_list if len(coll) > 0]
    return filter_list


def get_query_url(word):
    
    return BASE_URL + quote_plus('learners') + '/json/' + quote_plus(word) +\
            "?key=" + quote_plus(KEY)