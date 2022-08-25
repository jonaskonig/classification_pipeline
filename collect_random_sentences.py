from random import sample
from math import floor
import os

from helper import absoluteFilePaths, get_line_count, get_random_indices

sentences_file = open('random_sentences.txt')

directory = '/mnt/ceph/storage/data-tmp/teaching-current/jk76qufi/pushshift_reddit_dump'
file_paths = list(absoluteFilePaths(directory))

n_files = 5
random_files = sample(file_paths, n_files)

n_posts = 100
posts_per_file = floor(n_posts / n_files)

for file in files:
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
        

def split_sentences(text) -> list:
    split = re.split(self.split_pattern, text)
    split = [re.sub(self.regex, "", s) for s in split]
    split = [ re.sub(r"\s+", ' ', s) for s in split]
    return split

def find_whitespaces(sef, text) -> int:
    return len(re.findall(r'\w+', text))