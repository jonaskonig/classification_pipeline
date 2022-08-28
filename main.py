from detect_pipeline import DetectPipeline
from helper import absoluteFilePaths
from twitter_pipeline import TwitterPipeline
from reddit_pipeline import RedditPipeline


year_month = ['/classification_pipeline/newdownload/archiveteam-twitter-stream-2020-06', '/classification_pipeline/newdownload/archiveteam-twitter-stream-2020-11', '/classification_pipeline/newdownload/archiveteam-twitter-stream-2020-07', '/classification_pipeline/newdownload/archiveteam-twitter-stream-2020-12', '/classification_pipeline/newdownload/archiveteam-twitter-stream-2020-08', '/classification_pipeline/newdownload/archiveteam-twitter-stream-2021-01', '/classification_pipeline/newdownload/archiveteam-twitter-stream-2021-06', '/classification_pipeline/newdownload/archiveteam-twitter-stream-2020-09', '/classification_pipeline/newdownload/archiveteam-twitter-stream-2021-02', '/classification_pipeline/newdownload/archiveteam-twitter-stream-2020-10', '/classification_pipeline/newdownload/archiveteam-twitter-stream-2021-03']
dirlist = ["/classification_pipeline/newdownload/archiveteam-twitter-stream-2021-04","/classification_pipeline/newdownload/archiveteam-twitter-stream-2021-05","/classification_pipeline/newdownload/archiveteam-twitter-stream-2021-06"]
def startreddit():
    detect_pipeline = DetectPipeline()
    reddit_pipeline = RedditPipeline('sample_id', detect_pipeline)
    directory = '/mnt/ceph/storage/data-tmp/teaching-current/jk76qufi/pushshift_reddit_dump'
    file_paths = list(absoluteFilePaths(directory))
    reddit_pipeline.run(file_paths)


def starttwitter():
    detect_pipeline = DetectPipeline()
    twitter_pipeline = TwitterPipeline('sample_id', detect_pipeline, 50000, year_month)


if __name__ == '__main__':
    starttwitter()
