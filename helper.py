import os
import random
from statistics import mean, median
from typing import List

def absoluteFilePaths(directory):
    for dirpath,_,filenames in os.walk(directory):
        for f in filenames:
            yield os.path.abspath(os.path.join(dirpath, f))

def get_line_count(file_path) -> int:
        with open(file_path) as f:
            line_count = 0
            for line in f:
                line_count += 1
        return line_count

def get_random_indices(line_count, number) -> List[str]:
    return [random.randint(0, line_count - 1) for i in range(number)]

def count_all_lines():
    directory = '/mnt/ceph/storage/data-tmp/teaching-current/jk76qufi/pushshift_reddit_dump'
    file_paths = list(absoluteFilePaths(directory))
    with open('files_line_count.txt', 'w') as file:
        line_counts = []
        for file_path in file_paths:
            line_count = get_line_count(file_path)
            line_counts.append(line_count)
            file.write(file_path + ':' + str(line_count))
        with open('files_line_count_summary.txt', 'w') as file:
            file.write('Number of files: ' + str(len(file_paths)))
            file.write('Min number of lines: ' + str(min(line_counts)))
            file.write('Max number of lines: ' + str(max(line_counts)))
            file.write('Mean number of lines: ' + str(mean(line_counts)))
            file.write('Median number of lines: ' + str(median(line_counts)))
    