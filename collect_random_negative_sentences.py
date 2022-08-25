from random import sample
from math import floor
import os
import json
import re
from transformers import pipeline

from helper import absoluteFilePaths, get_line_count, get_random_indices

split_pattern = '(?<!\w\.\w.)(?<![A-Z][a-z]\.)(?<=\.|\?)\s'
rereplace = [r"\n", r"\r", r"RT", r"(https?.\S+)", r"\<(.*?)\>", '[^\x00-\x7F]']
regex = re.compile('|'.join(rereplace))

def split_sentences(text) -> list:
    split = re.split(split_pattern, text)
    split = [re.sub(regex, "", s) for s in split]
    split = [ re.sub(r"\s+", ' ', s) for s in split]
    return split

def find_whitespaces(text) -> int:
    return len(re.findall(r'\w+', text))


statement_question_bert = pipeline("text-classification",
                                            model="jonaskoenig/xtremedistil-l6-h256-uncased-question-vs-statement-classifier",
                                            tokenizer="jonaskoenig/xtremedistil-l6-h256-uncased-question-vs-statement-classifier",
                                            binary_output=True)
future_bert = pipeline("text-classification",
                                   model="jonaskoenig/xtremedistil-l6-h256-uncased-future-time-references-D1",
                                   tokenizer="jonaskoenig/xtremedistil-l6-h256-uncased-future-time-references-D1",
                                   binary_output=True)

sentences_file = open(os.path.join('random_sentences.txt'), 'a+')

directory = '/mnt/ceph/storage/data-tmp/teaching-current/jk76qufi/pushshift_reddit_dump'
file_paths = list(absoluteFilePaths(directory))

random_file = sample(file_paths, 1)

n_posts = 100

file = open(random_file[0], 'r')
line = file.readline()
index = 0

while index < n_posts:
    try:
        line = file.readline()
        reddit_post = json.loads(line)
        data = reddit_post["body"]

        sentences = split_sentences(data)
        cleaned_sentences = [x for x in sentences if 4 < find_whitespaces(x) < 35]
        not_future_statements = []
        for sentence in cleaned_sentences:
            if future_bert(sentence)[0]["label"] == "LABEL_0" and statement_question_bert(sentence)[0]["label"] == "LABEL_0" and sentence not in not_future_statements:
                not_future_statements.append(sentence)
                index += 1
        sentences_file.writelines(i + '\n' for i in not_future_statements)
    except ValueError as e:
        line_str = line if line else 'Line was empty'
        print(f'Parsing reddit post failed.\nLine: {line_str}\nError: {e}')