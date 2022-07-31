import os
from typing import List
import json
import logging

from detect_pipeline import DetectPipeline


class RedditPipeline:
    def __init__(self, detect_pipeline: DetectPipeline):
        self.detect_pipeline = detect_pipeline
        logging.basicConfig(filename='reddit_pipeline.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.INFO)

    def run(self, file_paths: List[str]):
        logging.info('Pipeline to run on the following files:\n' + str(file_paths) + '\n\n')
        logging.info('Pipeline running')

        current_progress = self.load_pipeline_progress()
        if current_progress != None:
            file_paths = file_paths[file_paths.index(current_progress) + 1:]
            logging.info('Reinitialized after progress: ' + str(current_progress))

        for file_path in file_paths:
            line_count = self.get_line_count(file_path)
            file = open(file_path, 'r')
            logging.info('File:\n' + str(file_path) + '\nLines: ' + str(line_count))
            reddit_post = json.loads(next(file))
            index = 0
            while reddit_post != None:
                data = reddit_post["body"]
                meta = self.extract_meta(reddit_post)
                self.detect_pipeline.datata_classify(data, meta)
                reddit_post = json.loads(next(file))
                if index % 100 == 0:
                    logging.info('Processed ' + str(index / line_count) + '% ' + ' (' + str(index) + '/' + str(line_count) + ')')
                index += 1
            self.save_pipeline_progress(file_path)
        logging.info('Pipeline finished')

    def load_pipeline_progress(self):
        path = 'reddit_pipeline_progress.txt'
        if os.path.exists(path):
            file = open(path, 'r')
            return json.loads(next(file))
        return None

    def save_pipeline_progress(self, file_path, index):
        file = open('reddit_pipeline_progress.txt', 'w')
        progress: dict = {file_path: file_path, index: index}
        file.write(json.dumps(progress))

    def extract_meta(self, reddit_post) -> dict:
        """TODO"""
        return reddit_post

    def get_line_count(self, file_path) -> int:
        with open(file_path) as f:
            line_count = 0
            for line in f:
                line_count += 1

        return line_count


