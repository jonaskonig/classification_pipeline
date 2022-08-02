from detect_pipeline import DetectPipeline
from helper import absoluteFilePaths

from reddit_pipeline import RedditPipeline

if __name__ == '__main__':
    detect_pipeline = DetectPipeline()
    reddit_pipeline = RedditPipeline('sample_id', detect_pipeline)
    directory = '/mnt/ceph/storage/data-tmp/teaching-current/jk76qufi/pushshift_reddit_dump'
    file_paths = list(absoluteFilePaths(directory))
    reddit_pipeline.run(file_paths)