import pytest
from mark_phonetic import preprocess_sentence

def test_preprocess_sentence():
    sent1 = "hello, World~ \n Hello USA \t(@qq.com), Hello Big5|Small5|JOHN"
    expect_sent1 = ['hello', 'world', 'hello', 'usa', 'qq', 'com', 'hello', 'john']

    sent2 = "I'm apple's, He'd, selled"
    expect_sent2 = ["i'm", 'apple', "he'd", "selled"]

    sent3 = '''his is a problem that has a particular computational structure. 
                You've got a set of options, you're going to choose one of those options, 
                and you're going to face exactly the same decision tomorrow. In that situation, 
                you run up against what computer scientists call the "explore-exploit trade-off."'''
    expect_sent3 = ["his", "is", "a", "problem", "that", "has", "a", "particular", "computational", "structure",
                    "you've", "got", "a", "set", "of", "options", "you're", "going", "to", "choose", "one", "of", "those", "options",
                    "and", "you're", "going", "to", "face", "exactly", "the", "same", "decision", "tomorrow", "in", "that", "situation",
                    "you", "run", "up", "against", "what", "computer", "scientists", "call", "the", "explore", "exploit", "trade", "off"]

    assert preprocess_sentence(sent1) == expect_sent1
    assert preprocess_sentence(sent2) == expect_sent2
    assert preprocess_sentence(sent3) == expect_sent3