import os
from typing import List
import json
import logging
import datetime

from detect_pipeline import DetectPipeline
from helper import get_line_count, get_random_indices


class RedditPipeline:
    def __init__(self, id: str, detect_pipeline: DetectPipeline):
        self.id = id
        self.detect_pipeline = detect_pipeline
        self.progress_file = f'reddit_pipeline_progress_{self.id}.txt'
        self.indices_file = f'reddit_pipeline_indices_{self.id}.txt'
        logging.basicConfig(filename=f'reddit_pipeline_{self.id}.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.INFO)
        self.posts_per_file = 10000

    def run(self, file_paths: List[str]):
        logging.info('Pipeline to run on the following files:\n' + str(file_paths) + '\n\n')
        logging.info('Pipeline running')

        current_progress = self.load_pipeline_progress()
        if current_progress != None:
            last_file = current_progress['file_path']
            file_paths = file_paths[file_paths.index(last_file):]
            start_index = current_progress['index']
            logging.info(f'Reinitialized in file:\n{last_file}\nat index {start_index}')
            random_indices = self.load_indices(last_file, start_index)
        else:
            random_indices = None

        counter = 0
        for file_path in file_paths:
            line_count = get_line_count(file_path)
            logging.info('\n\nProcessing new File:\n' + str(file_path) + '\nLines: ' + str(line_count) + '\n')

            if random_indices == None:
                random_indices = get_random_indices(line_count, self.posts_per_file)
                random_indices.sort()
                self.save_indices(file_path, random_indices)

            file = open(file_path, 'r')
            line = file.readline()
            current_index = 0

            for random_index in random_indices:
                while current_index < random_index:
                    line = file.readline()
                    current_index += 1

                reddit_post = json.loads(line)
                data = reddit_post["body"]
                meta = self.extract_meta(reddit_post)

                self.detect_pipeline.datata_classify(data, meta)
                self.save_pipeline_progress(file_path, random_index)

                counter += 1
                if counter % 10000 == 0:
                    logging.info('Processed ' + str(counter / line_count) + '% ' + ' (' + str(counter) + '/' + str(line_count) + ')')
                
            random_indices = None
        logging.info('Pipeline finished')

    def load_pipeline_progress(self):
        if os.path.exists(self.progress_file):
            file = open(self.progress_file, 'r')
            return json.loads(next(file))
        return None

    def save_pipeline_progress(self, file_path, index):
        with open(self.progress_file, 'w') as file:
            progress: dict = {'file_path': file_path, 'index': index}
            file.write(json.dumps(progress))

    def load_indices(self, file_path, start_index):
        if os.path.exists(self.indices_file):
            file = open(self.indices_file, 'r')
            for line in file:
                index_dict = json.loads(line)
                if index_dict['file_path'] == file_path:
                    indices = index_dict['indices']
                    return indices[indices.index(start_index):]
        return None

    def save_indices(self, file_path, indices):
        with open(self.indices_file, 'a') as file:
            progress: dict = {'file_path': file_path, 'indices': indices}
            file.write(json.dumps(progress))

    def extract_meta(self, reddit_post) -> dict:
        timestamp = datetime.datetime.fromtimestamp(reddit_post['created_utc'])
        return {
            'meta': {
                'timestamp': timestamp,
                'identifier': {
                    'permalink': reddit_post['permalink'],
                    'link_id': reddit_post['link_id'],
                    'parent_id': reddit_post['parent_id']
                },
                'subreddit': reddit_post['subreddit'],
                'source': 'REDDIT'
            }
        }
