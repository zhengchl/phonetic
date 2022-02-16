import re

# split by all punc except '
SPLIT_PUNC = re.compile(r'[-\s`~!@#$%^&*()_=+[{\]}\\|;:",<.>/?“”]+')
NUMBER = re.compile(r'\d+')

SINGLE_QUATE_WORD = set(('i', 'you', 'he', 'she', 'it',
                         'can', 'could', 'would', 'will',
                         'has', 'have',
                         'do', 'did', 'does',
                         'what', 'when', 'where', 'how'))


def preprocess_sentence(sent: str):
    raw_word_list = SPLIT_PUNC.split(sent)
    word_list = []
    for word in raw_word_list:
        query_word = word.lower() if word != 'I' else word

        if query_word == '':
            continue
        if NUMBER.search(query_word):
            continue

        if "'" in query_word:
            prefix = query_word.split("'")[0]
            if prefix.lower() not in SINGLE_QUATE_WORD:
                query_word = prefix
        word_list.append(query_word)

    return word_list
