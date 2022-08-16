import logging
import os
import json
import sys
import tarfile
import random
import multiprocessing
from datetime import datetime
import wget
import bz2

from detect_pipeline import DetectPipeline


class TwitterPipeline:
    def __init__(self, id: str, detect_pipeline: DetectPipeline, post_per_month: int, month_year_ist):
        self.id = id
        self.month_year_ist = month_year_ist
        if not os.path.exists(self.id):
            os.mkdir(self.id)
        self.detect_pipeline = detect_pipeline
        self.post_per_month = post_per_month
        self.progress_file = os.path.join(id, f'twitter_pipeline_progress.txt')
        # self.indices_file = os.path.join(id, f'twitter_pipeline_indices.txt')
        logging.basicConfig(
            filename=os.path.join(id, f'twitter_pipeline.log'),
            filemode='a',
            format='%(asctime)s - %(message)s',
            level=logging.INFO)
        progess = self.load_pipeline_progress()
        if progess:
            index = self.month_year_ist.index(progess["year"], progess["month"])
            self.month_year_ist = self.month_year_ist[index + 1:]
        start = self.month_year_ist.pop(0)
        self.yearmount = start
        if os.path.isfile(f"used/{start[0]}-{start[1]}.tar"):
            os.rename(f"used/{start[0]}-{start[1]}.tar", os.path.join("download/", f"{start[0]}-{start[1]}.tar"))
        if not os.path.isfile(f"download/{start[0]}-{start[1]}.tar"):
            self.getfile(start[0], start[1], f"download/{start[0]}-{start[1]}.tar")
        while len(self.month_year_ist) > 0:
            if progess:
                self.openfiles("download/", progess["file_name"])
                progess = None
            else:
                self.openfiles("download/")

    def bar_progress(self, current, total, width=80):
        progress_message = "Downloading: %d%% [%d / %d] bytes" % (current / total * 100, current, total)
        # Don't use print() as it will print in new line every time.
        sys.stdout.write("\r" + progress_message)
        sys.stdout.flush()

    def getfile(self, year, month, filename):
        month = str(month).zfill(2)
        url = f'https://archive.org/download/archiveteam-twitter-stream-{year}-{month}/archiveteam-twitter-{year}-{month}.tar'
        print(url)
        wget.download(url, filename, bar=self.bar_progress)

    def get_important_data(self, raw_json: str | bytes):
        data = json.loads(raw_json)
        try:

            meta = {"meta": {"timestamp": datetime.strptime(data["created_at"], '%a %b %d %H:%M:%S %z %Y'),
                             "identifier": {"id": data["id"], "userID": data["user"]["id"],
                                            "username": data["user"]["screen_name"]}, "source": "TWITTER"}}
            return data["text"], meta
        except Exception as e:
            return None, None

    def openfiles(self, filedir, startfile=None):
        filename = f"{self.yearmount[0]}-{self.yearmount[1]}.tar"
        os.rename(os.path.join(filedir, filename), os.path.join("used", f"{self.yearmount[0]}-{self.yearmount[1]}.tar"))
        next = self.month_year_ist.pop(0)
        p1 = multiprocessing.Process(target=self.getfile, args=(next[0], next[1], f"download/{next[0]}-{next[1]}.tar",))
        p1.start()
        tar = tarfile.open(os.path.join("used", filename))
        filenames = tar.getnames()
        filenames = [x for x in filenames if x.endswith(".json.bz2")]
        perfile = int(self.post_per_month / len(filenames))
        print(f"per file: {perfile}")
        takewith = 0
        if startfile:
            index = filenames.index(startfile)
            filenames = filenames[index + 2:]
        for f in filenames:
            self.save_pipeline_progress(f,self.yearmount[0], self.yearmount[1])
            thisjson = bz2.open(tar.extractfile(f))
            lines = thisjson.readlines()
            collectiontext = []
            if len(lines) + takewith < perfile:
                takewith += perfile - len(lines)
                for line in lines:
                    text, meta = self.get_important_data(line)
                    if text:
                        collectiontext.append((text, meta))
                    if len(collectiontext) > 9:
                        self.detect_pipeline.data_list_classify(collectiontext)

                        collectiontext = []
            else:
                linenumberlist = random.sample(range(0, len(lines)), perfile + takewith)
                takewith = 0
                for linenumber in linenumberlist:
                    text, meta = self.get_important_data(lines[linenumber])
                    if text:
                        collectiontext.append((text, meta))
                    if len(collectiontext) > 9 or len(collectiontext) == len(linenumberlist):
                        self.detect_pipeline.data_list_classify(collectiontext)

                        collectiontext = []
        os.rename("stats.txt", f"{self.yearmount[0]}-{self.yearmount[1]}.txt")

        self.yearmount = next

        os.remove(os.path.join("used", filename))
        p1.join()

    def save_pipeline_progress(self, file_name, year, month):
        with open(self.progress_file, 'w') as file:
            progress: dict = {'file_name': file_name, 'year': year, 'month': month}
            file.write(json.dumps(progress))

    def load_pipeline_progress(self) -> dict | None:
        if os.path.exists(self.progress_file):
            file = open(self.progress_file)
            return json.loads(file.read())
        return None
