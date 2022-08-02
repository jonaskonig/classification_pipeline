from math import floor
from detect_pipeline import DetectPipeline
from reddit_pipeline import RedditPipeline
from helper import absoluteFilePaths


id = 'pipeline_3_of_3'
detect_pipeline = DetectPipeline()
reddit_pipeline = RedditPipeline(id, detect_pipeline)

directory = '/mnt/ceph/storage/data-tmp/teaching-current/jk76qufi/pushshift_reddit_dump'
file_paths = list(absoluteFilePaths(directory))

file_paths_count = len(file_paths)
start = floor(file_paths_count / 3) * 2
end = file_paths_count
file_paths = file_paths[start:end]

reddit_pipeline.run(file_paths)