import pytest

from mark_phonetic import split_collection
from mark_phonetic import get_query_url

def flatten(list_2d):
    rtn = []
    for l in list_2d:
        rtn.extend(l)
    return rtn

def test_split_collection():
    test_list1 = list(i * 2 + 1 for i in range(6))
    test_list2 = list(i * 2 + 1 for i in range(7))
    test_list3 = list(i * 2 + 1 for i in range(8))
    test_set1 = set(test_list1)
    test_set2 = set(test_list2)
    test_set3 = set(test_list3)

    expect_out1 = [[1, 3], [5, 7], [9, 11]]
    expect_out2 = [[1, 3, 5], [7, 9, 11], [13]]
    expect_out3 = [[1, 3, 5], [7, 9, 11], [13, 15]]

    assert split_collection(test_list1, 3) == expect_out1
    assert split_collection(test_list2, 3) == expect_out2
    assert split_collection(test_list3, 3) == expect_out3

    out1 = split_collection(test_set1, 3)
    out2 = split_collection(test_set2, 3)
    out3 = split_collection(test_set3, 3)

    assert len(out1) == 3
    assert len(out2) == 3
    assert len(out3) == 3

    assert sorted(flatten(out1)) == test_list1
    assert sorted(flatten(out2)) == test_list2
    assert sorted(flatten(out3)) == test_list3

    assert split_collection(test_list1, 100) == [[i] for i in test_list1]
    
    assert split_collection([], 1000) == []
