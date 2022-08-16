from detect_pipeline import DetectPipeline
from helper import absoluteFilePaths
from twitter_pipeline import TwitterPipeline
from reddit_pipeline import RedditPipeline

#year_month = [(2018, 4), (2018, 3), (2018, 2), (2018, 1), (2017, 11), (2017, 10), (2017, 9), (2017, 8), (2017, 7), (2017, 6), (2017, 5), (2017, 4), (2017, 3), (2017, 2), (2017, 1), (2016, 12), (2016, 11), (2016, 10), (2016, 9), (2016, 8), (2016, 7), (2016, 6), (2016, 5), (2016, 4), (2016, 3), (2016, 2), (2016, 1), (2015, 12), (2015, 11), (2015, 10), (2015, 9), (2015, 8), (2015, 7), (2015, 6), (2015, 5), (2015, 4), (2015, 3), (2014, 12), (2014, 11), (2014, 10), (2014, 9), (2014, 8), (2014, 7), (2014, 6), (2014, 5), (2014, 4), (2014, 3), (2014, 2), (2014, 1), (2014, 1), (2013, 12), (2013, 11), (2013, 10), (2013, 9), (2013, 8), (13, 7), (2013, 6), (2013, 5), (2013, 4), (2013, 3), (2013, 2), (2013, 1), (2012, 12), (2012, 11), (2012, 10), (2012, 9), (2012, 8), (2012, 7), (2012, 6), (2012, 5), (2012, 4), (2012, 3), (2012, 2), (2012, 1)]
year_month = [(2018, 4), (2018, 3), (2018, 2), (2018, 1), (2017, 11), (2017, 10), (2017, 9), (2017, 8), (2017, 7), (2017, 6), (2017, 5), (2017, 4), (2017, 3), (2017, 2), (2017, 1), (2016, 12), (2016, 11), (2016, 10), (2016, 9), (2016, 8), (2016, 7), (2016, 6), (2016, 5), (2016, 4), (2016, 3), (2016, 2), (2016, 1), (2015, 12), (2015, 11), (2015, 10), (2015, 9), (2015, 8), (2015, 7), (2015, 6), (2015, 5), (2015, 4), (2015, 3)]
year_month.reverse()
def startreddit():
    detect_pipeline = DetectPipeline()
    reddit_pipeline = RedditPipeline('sample_id', detect_pipeline)
    directory = '/mnt/ceph/storage/data-tmp/teaching-current/jk76qufi/pushshift_reddit_dump'
    file_paths = list(absoluteFilePaths(directory))
    reddit_pipeline.run(file_paths)


def starttwitter():
    detect_pipeline = DetectPipeline()
    twitter_pipeline = TwitterPipeline('sample_id', detect_pipeline, 100000, year_month)


if __name__ == '__main__':
    starttwitter()
