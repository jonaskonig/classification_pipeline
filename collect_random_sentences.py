from random import sample
from math import floor
import os
import json
import re

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

sentences_file = open(os.path.join('random_sentences.txt'), 'a+')

directory = '/mnt/ceph/storage/data-tmp/teaching-current/jk76qufi/pushshift_reddit_dump'
file_paths = list(absoluteFilePaths(directory))

n_files = 5
random_files = sample(file_paths, n_files)

n_posts = 100
posts_per_file = floor(n_posts / n_files)

for file_path in file_paths:
    line_count = get_line_count(file_path)
    random_indices = get_random_indices(line_count, posts_per_file)

    file = open(file_path, 'r')
    line = file.readline()
    current_index = 0

    for random_index in random_indices:
        while current_index < random_index:
            line = file.readline()
            current_index += 1

        try:
            reddit_post = json.loads(line)
            data = reddit_post["body"]

            sentences = split_sentences(data)
            cleaned_sentences = [x for x in sentences if 4 < find_whitespaces(x) < 35]

            sentences_file.writelines("%s\n" % i for i in cleaned_sentences)

        except ValueError as e:
            line_str = line if line else 'Line was empty'
            print(f'Parsing reddit post failed.\nLine: {line_str}\nError: {e}')
