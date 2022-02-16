import json

from mark_phonetic import MwLearnerParser


def test_mw_dict_parser():
    learners_choose_text = open(
        'test/resources/learners_choose.json', 'r').read()
    choose_parser = MwLearnerParser('choose', learners_choose_text, 'web')
    out_json = choose_parser.json
    assert len(out_json) == 1
    assert out_json[0]['meta']['id'] == 'choose'
    assert choose_parser.is_valid
    assert len(choose_parser.prs_list) == 1
    assert choose_parser.prs_list[0] == "\u02c8t\u0283u\u02d0z"

    choose_parser = MwLearnerParser('choose', learners_choose_text, 'db')
    out_json = choose_parser.json
    out_text = choose_parser.dumps()
    assert len(out_json) == 5
    assert out_json[0]['meta']['id'] == 'choose'
    assert choose_parser.is_valid
    assert len(choose_parser.prs_list) == 4
    assert set(choose_parser.prs_list) == {"\u02c8t\u0283u\u02d0z",
                                           "\u02c8p\u026ak",
                                           "\u02c8po\u026azn\u0329",
                                           "\u02c8sa\u026ad"}
    assert json.loads(out_text) == json.loads(learners_choose_text)

    learners_test_text = open('test/resources/learners_test.json', 'r').read()
    test_parser = MwLearnerParser('test', learners_test_text, 'web')
    out_json = test_parser.json
    assert len(out_json) == 2
    assert out_json[0]['meta']['id'] == 'test:1'
    assert out_json[1]['meta']['id'] == 'test:2'
    assert test_parser.is_valid
    assert len(test_parser.prs_list) == 1
    assert test_parser.prs_list[0] == 'ˈtɛst'
    assert out_json == json.loads(test_parser.dumps())

    learners_cities_text = open(
        'test/resources/learners_cities.json', 'r').read()
    cities_parser = MwLearnerParser('cities', learners_cities_text, 'web')
    out_json = cities_parser.json
    assert len(out_json) == 1
    assert out_json[0]['meta']['id'] == 'city'
    assert cities_parser.is_valid
    assert len(cities_parser.prs_list) == 1
    assert cities_parser.prs_list[0] == "\u02c8s\u026ati"
    assert out_json == json.loads(cities_parser.dumps())

    learners_asdf_text = open('test/resources/learners_asdf.json', 'r').read()
    asdf_parser = MwLearnerParser('asdf', learners_asdf_text, 'web')
    out_text = asdf_parser.dumps()
    assert out_text == '[]'
    assert asdf_parser.is_valid == False
    assert asdf_parser.prs_list == []
    assert asdf_parser.prs_list == []
