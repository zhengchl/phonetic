import pytest
import asyncio
import aiosqlite
import aiohttp

from mark_phonetic import mark_phonetic
from mark_phonetic.mw_dict_parser import MwLearnerParser

DB_FILE = ':memory:'
INPUT_FILE = 'test/resources/input_file'


async def init_db(db: aiosqlite.Connection):
    await db.execute('''CREATE TABLE IF NOT EXISTS mw_raw
                    (
                        "word" TEXT  PRIMARY KEY NOT NULL,
                        "learners" TEXT
                    )
                '''
                     )
    table_values = [["you're", "you're_l"],
                    ["hello", "hello_l"],
                    ["run", "run_l"],
                    ["problem", "problem_l"],
                    ["mama", "mama_l"],
                    ]
    await db.executemany("insert into mw_raw values(?, ?)", table_values)


async def prepare_wrapper():
    expected_raw_input = open(INPUT_FILE).read()
    expected_word_list = ["his", "is", "a", "problem", "that", "has", "a", "particular", "computational", "structure",
                          "you've", "got", "a", "set", "of", "options", "you're", "going", "to", "choose", "one", "of", "those", "options",
                          "and", "you're", "going", "to", "face", "exactly", "the", "same", "decision", "tomorrow", "in", "that", "situation",
                          "you", "run", "up", "against", "what", "computer", "scientists", "call", "the", "explore", "exploit", "trade", "off",
                          "life", "you're", "you", "is"]
    expected_db_set = {"you're", "run", "problem"}
    expected_web_set = {"his", "is", "a", "that", "has", "a", "particular", "computational", "structure",
                        "you've", "got", "a", "set", "of", "options", "going", "to", "choose", "one", "of", "those", "options",
                        "and", "going", "to", "face", "exactly", "the", "same", "decision", "tomorrow", "in", "that", "situation",
                        "you", "up", "against", "what", "computer", "scientists", "call", "the", "explore", "exploit", "trade", "off",
                        "life", "you", "is"}

    async with aiosqlite.connect(DB_FILE) as db:
        await init_db(db)
        raw_input, word_list, db_word_set, web_word_set = await mark_phonetic.prepare(db, INPUT_FILE)
        assert raw_input == expected_raw_input
        assert word_list == expected_word_list
        assert db_word_set == expected_db_set
        assert web_word_set == expected_web_set


def test_prepare():
    asyncio.run(prepare_wrapper())


async def db_query_producer_wrapper():
    expected_out = sorted([("db_queryer_0", "you're", "you're_l"),
                           ("db_queryer_0", "run", "run_l"),
                           ("db_queryer_0", "problem", "problem_l")])

    word_set = {"you're", "run", "problem"}
    word_q = asyncio.Queue()
    async with aiosqlite.connect(DB_FILE) as db:
        await init_db(db)
        await mark_phonetic.db_query_producer(0, db, word_set, word_q)
    out1 = await word_q.get()
    out2 = await word_q.get()
    out3 = await word_q.get()
    out = sorted([out1, out2, out3])
    assert out == expected_out
    assert word_q.empty()


def test_db_query_producer():
    asyncio.run(db_query_producer_wrapper())


async def parser_consumer_wrapper():
    word_q = asyncio.Queue()
    prs_dict = dict()
    learners_choose_text = open(
        'test/resources/learners_choose.json', 'r').read()
    learners_cities_text = open(
        'test/resources/learners_cities.json', 'r').read()
    learners_asdf_text = open('test/resources/learners_asdf.json', 'r').read()
    await word_q.put(('db_queryer_0', 'choose', learners_choose_text))
    await word_q.put(('web_queryer_0', 'cities', learners_cities_text))
    await word_q.put(('db_queryer_0', 'asdf', learners_asdf_text))
    task = asyncio.create_task(
        mark_phonetic.parser_consumer(0, word_q, prs_dict))
    await word_q.join()
    task.cancel()
    assert len(prs_dict) == 2
    assert 'choose' in prs_dict
    assert 'cities' in prs_dict
    assert 'asdf' not in prs_dict
    assert len(prs_dict['choose']) == 4
    assert set(prs_dict['choose']) == {"\u02c8t\u0283u\u02d0z",
                                       "\u02c8p\u026ak",
                                       "\u02c8po\u026azn\u0329",
                                       "\u02c8sa\u026ad"}
    assert len(prs_dict['cities']) == 1
    assert prs_dict['cities'][0] == "\u02c8s\u026ati"


def test_parser_consumer():
    asyncio.run(parser_consumer_wrapper())


async def db_writer_consumer_wapper():
    writer_q = asyncio.Queue()
    word_q = asyncio.Queue()
    with open('test/resources/learners_test.json', 'r') as fid:
        learners_test_text = fid.read()
    await writer_q.put(('web_queryer_0', 'test', learners_test_text))

    async with aiosqlite.connect(DB_FILE) as db:
        await init_db(db)
        task = asyncio.create_task(
            mark_phonetic.db_writer_consumer(0, db, writer_q))
        await writer_q.join()
        task.cancel()

        word_set = {'test'}
        await mark_phonetic.db_query_producer(0, db, word_set, word_q)
        test_info = await word_q.get()
        assert test_info[0] == 'db_queryer_0'
        assert test_info[1] == 'test'
        assert test_info[2] == MwLearnerParser('test', learners_test_text, 'web').dumps()
        assert word_q.empty()


def test_db_writer_consumer():
    asyncio.run(db_writer_consumer_wapper())


async def web_query_prodecer_wrapper():
    word_set = {'test'}
    writer_q = asyncio.Queue()
    word_q = asyncio.Queue()

    learners_test_text = open('test/resources/learners_test.json', 'r').read()
    async with aiohttp.ClientSession() as session:
        await mark_phonetic.web_query_prodecer(0, session, word_set, word_q, writer_q)
        task_name1, word1, out11 = await word_q.get()
        task_name2, word2, out21 = await writer_q.get()
        assert task_name1 == 'web_queryer_0'
        assert task_name2 == 'web_queryer_0'
        assert word1 == 'test'
        assert word2 == 'test'
        assert out11 == learners_test_text
        assert out21 == learners_test_text

# cancel this test because of key is private
# def test_web_query_prodecer():
#     asyncio.run(web_query_prodecer_wrapper())
