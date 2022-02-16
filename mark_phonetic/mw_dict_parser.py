import json


class ParserBase:
    def __init__(self, word, json_text, source):
        self._json = json.loads(json_text)
        self._word = word
        self._is_valid = None
        self._prs_list = None

        if source == 'web':
            self._json = self.filter_homograph()

    def filter_homograph(self):
        '''filter homograph'''
        filter_fun = (lambda entry:
                        'meta' in entry and
                        'id' in entry['meta'] and
                        entry['meta']['id'].partition(':')[0] == self._word)
        filter_entry = [entry for entry in self._json if filter_fun(entry)]
        if len(filter_entry) == 0:
            '''word is Inflection, like 'cities', so all entry is filtered'''
            inflection_filter_fun = (lambda entry: 
                                        'meta' in entry and
                                        'stems' in entry['meta'] and
                                        self._word in entry['meta']['stems'])
            filter_entry = [entry for entry in self._json if inflection_filter_fun(entry)]
        return filter_entry

    def dumps(self):
        return json.dumps(self._json, indent=None)

    @property
    def json(self):
        return self._json

class MwLearnerParser(ParserBase):
    def __init__(self, word, json_text, source):
        ParserBase.__init__(self, word, json_text, source)
        
    @property
    def prs_list(self):
        if not self.is_valid:
            self._prs_list = []
            return []
        if not self._prs_list is None:
            return self._prs_list
        raw_prs_list = []
        for entry in self._json:
            if 'hwi' in entry:
                if 'prs' in entry['hwi']:
                    raw_prs_list.extend(entry['hwi']['prs'])
                if 'altprs' in entry['hwi']:
                    raw_prs_list.extend(entry['hwi']['altprs'])
        prs_set = set()
        for prs_info in raw_prs_list:
            if 'ipa' in prs_info:
                prs_set.add(prs_info['ipa'])
        self._prs_list = list(prs_set)
        return self._prs_list
    
    @property
    def is_valid(self):
        if not self._is_valid is None:
            return self._is_valid
        for entry in self._json:
            if 'meta' in entry:
                self._is_valid = True
                break
        else:
            self._is_valid = False
        return self._is_valid
        