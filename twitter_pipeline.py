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
import zipfile
from detect_pipeline import DetectPipeline

FILEDIR = "/classification_pipeline/newdie/archiveteam-json-twitterstream/"
class TwitterPipeline:
    def __init__(self, id: str, detect_pipeline: DetectPipeline, post_per_month: int, Dirlist):
        self.id = id
        self.Dirlist = Dirlist
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
            start = Dirlist.index(progess["tardir"])
            Dirlist  = Dirlist[start+1:]
        for dir in Dirlist:
            filenemaes = [x for x in os.listdir(dir) if x.endswith(".zip") or x.endswith(".tar")]
            if progess:
                start = filenemaes.index(progess["tarfilename"])
                filenemaes = filenemaes[start :]
                progess = None
            for f in filenemaes:
                if progess:
                    self.openfiles(dir, f, progess["file_name"])
                else:
                    self.openfiles(dir, f)


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

        try:
            data = json.loads(raw_json)
            meta = {"meta": {"timestamp": datetime.strptime(data["created_at"], '%a %b %d %H:%M:%S %z %Y'),
                             "identifier": {"id": data["id"], "userID": data["user"]["id"],
                                            "username": data["user"]["screen_name"]}, "source": "TWITTER"}}
            return data["text"], meta
        except Exception as e:
            return None, None

    def openfiles(self,dir, filename, startfile=None):
        #filename = f"{self.yearmount[0]}-{self.yearmount[1]}.tar"
        #os.rename(os.path.join(filedir, filename), os.path.join("used", f"{self.yearmount[0]}-{self.yearmount[1]}.tar"))
        #next = self.month_year_ist.pop(0)
        #p1 = multiprocessing.Process(target=self.getfile, args=(next[0], next[1], f"download/{next[0]}-{next[1]}.tar",))
        #p1.start()v

        if filename.endswith(".zip"):
            tar = zipfile.ZipFile(os.path.join(dir, filename))
            filenames = tar.namelist()
        elif filename.endswith(".tar"):
            tar = tarfile.open(os.path.join(dir,filename))
            filenames = tar.getnames()
        else:
            return None
        filenames = [x for x in filenames if x.endswith(".json.bz2")]
        if len(filenames) < 1:
            return None
        perfile = int(self.post_per_month / len(filenames))
        perfile = perfile if perfile > 0 else 1
        print(f"per file: {perfile}")
        takewith = 0
        if startfile:
            index = filenames.index(startfile)
            filenames = filenames[index + 2:]
        for f in filenames:
            self.save_pipeline_progress(f,filename, dir)
            try:
                if filename.endswith(".zip"):
                    thisjson = bz2.open(tar.open(f))
                elif filename.endswith(".tar"):
                    thisjson = bz2.open(tar.extractfile(f))
                else:
                    continue
            except:
                continue
            lines = thisjson.readlines()
            if len(lines)<1:
                continue
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
        os.rename("stats.txt", filename)




    def save_pipeline_progress(self, file_name, tarfile, tardir):
        with open(self.progress_file, 'w') as file:
            progress: dict = {'file_name': file_name, "tarfilename":tarfile, "tardir": tardir}
            file.write(json.dumps(progress))

    def load_pipeline_progress(self) -> dict | None:
        if os.path.exists(self.progress_file):
            file = open(self.progress_file)
            return json.loads(file.read())
        return None
