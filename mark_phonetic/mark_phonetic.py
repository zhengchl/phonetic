import logging
from itertools import chain

import aiohttp
import aiosqlite
import aiofiles
import asyncio

from mark_phonetic import preprocess_sentence
from mark_phonetic import MwLearnerParser
from mark_phonetic import split_collection
from mark_phonetic import get_query_url
from mark_phonetic.common import read_key

DB_FILE = 'dict.db'

DB_PRODUCER_NUM = 5
WEB_PRODUCER_NUM = 5
PARSER_CONSUMER_NUM = 10
WRITER_CONSUMER_NUM = 5


async def get_cache_word_set(db: aiosqlite.Connection) -> set:
    '''get cached words set from db'''
    await db.execute('''CREATE TABLE IF NOT EXISTS mw_raw
                        (
                            "word" TEXT  PRIMARY KEY NOT NULL,
                            "learners" TEXT
                        )
                    '''
                    )
    async with db.execute('select word from mw_raw') as cursor:
        rows = await cursor.fetchall()
        word_set = set(row[0] for row in rows)
    return word_set


async def load_input_word_set(filename):
    async with aiofiles.open(filename, mode='r') as fid:
        raw_input = await fid.read()
    word_list = preprocess_sentence(raw_input)
    return raw_input, word_list


async def prepare(db: aiosqlite.Connection, filename: str):
    task1 = asyncio.create_task(get_cache_word_set(db))
    task2 = asyncio.create_task(load_input_word_set(filename))
    cache_db_word_set = (await asyncio.gather(task1))[0]
    raw_input, word_list = (await asyncio.gather(task2))[0]
    query_set = set(word_list)
    web_word_set = query_set.difference(cache_db_word_set)
    db_word_set = query_set.intersection(cache_db_word_set)
    assert(len(web_word_set.intersection(db_word_set)) == 0)
    assert(web_word_set.union(db_word_set) == query_set)
    logging.info(f"Prepare db query words number: {len(db_word_set)}, web query words number: {len(web_word_set)}")
    return raw_input, word_list, db_word_set, web_word_set


async def db_query_producer(task_idx: int,
                            db: aiosqlite.Connection,
                            word_set: set,
                            word_q: asyncio.Queue):
    task_name = f'db_queryer_{task_idx}'
    logging.info(f"Producer {task_name} inited," +
                 f" query db {len(word_set)} words.")
    sql = f"SELECT word, learners FROM mw_raw where word in ({', '.join('?' for _ in word_set)})"
    word_list = list(word_set)
    async with db.execute(sql, word_list) as cursor:
        async for row in cursor:
            await word_q.put((task_name, row[0], row[1]))
    logging.info(f"Producer {task_name} finished.")

async def web_query_prodecer(task_idx: int,
                             session: aiohttp.ClientSession,
                             word_set: set,
                             word_q: asyncio.Queue,
                             writer_q: asyncio.Queue):
    task_name = f'web_queryer_{task_idx}'
    logging.info(f"Producer {task_name} inited," +
                 f" query web {len(word_set)} words.")
    for word in word_set:
        url = get_query_url(word)
        resp_text = None
        async with session.get(url) as resp:
            if resp.status == 200:
                text = await resp.text()
                resp_text = text
        if resp_text:
            logging.info(f'Producer {task_name} read "{word}" sucessfully')
            await word_q.put((task_name, word, resp_text))
            await writer_q.put((task_name, word, resp_text))
    logging.info(f"Producer {task_name} finished.")


async def parser_consumer(task_idx: int, word_q: asyncio.Queue, prs_dict: dict):
    task_name = f'parser_consumer_{task_idx}'
    logging.info(f"Consumer {task_name} inited.")
    while True:
        queryer_task_name, word, learners = await word_q.get()
        if learners:
            if queryer_task_name.startswith("web_queryer_"):
                source = 'web'
            else:
                source = 'db'
            parser = MwLearnerParser(word, learners, source)
            if parser.is_valid:
                prs_dict[word] = parser.prs_list
                logging.info(f'Consumer {task_name} parse "{word}" sucessfully')
            else:
                logging.info(f'Consumer {task_name} parse "{word}", but "{word}" is invalid')
        word_q.task_done()


async def db_writer_consumer(task_idx: int, db: aiosqlite.Connection, writer_q: asyncio.Queue):
    task_name = f'writer_consumer_{task_idx}'
    logging.info(f"Consumer {task_name} inited.")
    while True:
        task_name, word, learners = await writer_q.get()
        assert task_name.startswith("web_queryer_")
        # write parser dumps instead of raw json text
        parser = MwLearnerParser(word, learners, 'web')
        await db.execute("INSERT INTO mw_raw VALUES (?, ?)", (word, parser.dumps()))
        logging.info(f'Consumer {task_name} write "{word}" sucessfully')
        writer_q.task_done()


async def write_result(prs_dict: dict, filename: str):
    logging.info(f'Write result task started')
    if filename.endswith('.txt'):
        out_filename = filename[:-4] + '_out.txt'
    else:
        out_filename = filename + '_out'
    async with aiofiles.open(out_filename, 'w') as ofid:
        for word in sorted(prs_dict.keys()):
            prs_str = ','.join(f'/{prs}/' for prs in prs_dict[word])
            await ofid.write(f'{word} {prs_str}\n')
    logging.info(f'Write out {len(prs_dict)} words to {out_filename}')


async def main(filename: str):
    read_key()

    word_q = asyncio.Queue()
    writer_q = asyncio.Queue()
    prs_dict = dict()
    timeout = aiohttp.ClientTimeout(total=60)
    async with aiosqlite.connect(DB_FILE) as db:
        # prepare word set, so must done before producer-consumer tasks
        raw_input, word_list, db_word_set, web_word_set = await prepare(db, filename)
        logging.info('Prepare task finished')

        async with aiohttp.ClientSession(timeout=timeout) as session:
            db_word_list = split_collection(db_word_set, DB_PRODUCER_NUM)
            web_word_list = split_collection(web_word_set, WEB_PRODUCER_NUM)

            # create producer-consumer tasks
            db_producer_task = [asyncio.create_task(db_query_producer(idx, db, word_set, word_q))
                                for (idx, word_set) in enumerate(db_word_list)]
            web_producer_task = [asyncio.create_task(web_query_prodecer(idx, session, word_set, word_q, writer_q))
                                 for (idx, word_set) in enumerate(web_word_list)]
            parser_consumer_task = [asyncio.create_task(parser_consumer(idx, word_q, prs_dict))
                                    for idx in range(PARSER_CONSUMER_NUM)]
            writer_consumer_task = [asyncio.create_task(db_writer_consumer(idx, db, writer_q))
                                    for idx in range(WRITER_CONSUMER_NUM)]

            await asyncio.wait(chain(db_producer_task, web_producer_task))
            logging.info('All producer task finished')
            await word_q.join()
            for task in parser_consumer_task:
                task.cancel()
            logging.info('All parser consumer task finished and canceled')

            # write_result must after word_q is empty
            await asyncio.gather(write_result(prs_dict, filename))
            logging.info('Write result task finished')

            await writer_q.join()
            for task in writer_consumer_task:
                task.cancel()
            logging.info('All writer consumer task finished and canceled')

            await db.commit()
            logging.info('DB committed')

