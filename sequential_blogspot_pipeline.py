

import sys
import csv
csv.field_size_limit(sys.maxsize)
import re
import os
from transformers import pipeline
import pymongo
import tensorflow as tf
tf.config.experimental.enable_tensor_float_32_execution(False)
import logging
from datetime import datetime



class BlogPipeline():
    
    def __init__(self):
        
        if sys.version_info[1] < 9 and sys.version_info[0] < 3:
            raise Exception("Must be using Python 3.9 or higher")
        
        self.PATH = "/mnt/ceph/storage/data-tmp/teaching-current/ms19hove/Datasets/blogspot/"
        
        # file paths
        self.source_file = f"{self.PATH}blogs_large_commoncrawl.csv"
        self.progressionfile = f"{self.PATH}progression.txt"
        
        # progression file
        if not os.path.exists(self.progressionfile):
            with open(self.progressionfile, "w") as f:
                f.write("0")
        
        logging.basicConfig(filename=f"{self.PATH}blogspot_pipeline.log",
                    filemode='a',
                    format='%(asctime)s - %(message)s',
                    level=logging.INFO) 
        
        # splitting sentences
        self.pattern = "(?<!\w\.\w.)(?<![A-Z][a-z]\.)(?<=\.|\?)\s"
        self.re_replace = [r"\n", r"\r", r"RT", r"(https?.\S+)", r"\<(.*?)\>", '[^\x00-\x7F]']
        self.regex = re.compile('|'.join(self.re_replace))

        # classification setup
        self.topic_threshold = 0.1
        self.emotion_threshold = 0.1
        self.label_count = 4
        self.topic_certainty = 0.5
        self.emotion_certainty = 0.2

        # db
        self.connect_mongo = "***REMOVED***" 
        self.myclient = pymongo.MongoClient(self.connect_mongo)
        self.ftr_s = self.myclient["ftr"]["Future_time_references"]
        self.ftr_q = self.myclient["ftr"]["Future_time_references_questions"]

        # berts
        self.topicbert = pipeline("text-classification", model="jonaskoenig/topic_classification_04",
                            tokenizer="jonaskoenig/topic_classification_04", top_k = self.label_count)
        
        self.emotionbert = pipeline("text-classification", model="jonaskoenig/xtremedistil-l6-h256-uncased-go-emotion",
                                    tokenizer="jonaskoenig/xtremedistil-l6-h256-uncased-go-emotion", top_k = self.label_count)
        
        self.statementvssentence = pipeline("text-classification",
                                            model="jonaskoenig/xtremedistil-l6-h256-uncased-question-vs-statement-classifier",
                                            tokenizer="jonaskoenig/xtremedistil-l6-h256-uncased-question-vs-statement-classifier",
                                            binary_output=True)
        
        self.futurebert = pipeline("text-classification",
                                   model="jonaskoenig/xtremedistil-l6-h256-uncased-future-time-references-D1",
                                   tokenizer="jonaskoenig/xtremedistil-l6-h256-uncased-future-time-references-D1",
                                   binary_output=True)

        

    def gen_data_stream(self, source_file) -> list:
        with open(source_file, "r", encoding="utf-8", errors="ignore") as f:
            reader = csv.reader(f)
            next(reader)
            for line in reader:
                if line != []: # skip empty lines
                    yield line
      
    def get_csv_data(self, line):
        text = line[0]
        url = line[1]
        timestamp = datetime.strptime(line[2], "%d/%m/%Y")
        comment = line[3]

        metadata = {
            "meta":{
                "timestamp":timestamp,
                "identifier":{
                    "permalink":url
                },
                "comment": comment,
                "source": "BLOGSPOT"
                }
            }
        return text, metadata
    
    def get_num_whitespace(self, text) -> int:
        return len(re.findall(r'\w+', text))

    def split_clean_text(self, text) -> list:
        split = re.split(self.pattern, text)
        split = [re.sub(self.regex, "", s) for s in split]
        split = [ re.sub(r"\s+", ' ', s) for s in split]
        split = [x for x in split if 4 < self.get_num_whitespace(x) < 35]
        return split

    def classify_sentence(self, sentence):
        
        fut = self.futurebert(sentence)

        # if sentence = ftr
        if fut[0]["label"] == "LABEL_1":    
            
            svs = self.statementvssentence( sentence )
            em = self.emotionbert( sentence, top_k = self.label_count )
            top = self.topicbert( sentence, top_k = self.label_count )

            if top[0]["score"] < self.topic_certainty or em[0]["score"] < self.emotion_certainty:
                return 2, None
            
            else:
                topl = [ x["label"] for x in top if top[0]["score"] - x["score"] < self.topic_threshold ]
                eml = [ x["label"] for x in em if em[0]["score"] - x["score"] < self.emotion_threshold ]
                em = [ x["score"] for x in em ]
                top = [ x["score"] for x in top ]
                
                output = {"text": sentence,
                        "emotion": eml,
                        "topic": topl,
                        "emotion_certainty": em[:len(eml)],
                        "topic_certainty": top[:len(topl)]
                        }
                
                if svs[0]["label"] == "LABEL_0":
                    
                    return 0, output                #  0 = future question  

                return 1, output                    #  1 = future statement
        
        return 2, None                          #  2 = no ftr, none

    def store_counter(self, actual_count):
        with open(self.progressionfile, "w") as f:
            f.write(str(actual_count))
    
    def read_counter(self):
        with open(self.progressionfile, "r") as f:
            c = int(f.read())
        return c

    def print_classifications(self, objects, mode):
        print(f"inserted into {mode}: ")
        print()
        print(objects)

    def run_pipeline(self):
        
        logging.info('Blogspot Pipeline started running')

        data = self.gen_data_stream(self.source_file)
        
        progression = 0
        checkpoint = self.read_counter()
        
        for item in data:

            text, meta = self.get_csv_data(item)
            
            sentences = self.split_clean_text(text)

            for sentence in sentences:
                
                # just classify if there was no break in pipeline
                if progression >= checkpoint:
                    try:
                        qvs, classifications = self.classify_sentence(sentence)
                        # if ftr --> store in db
                        try:
                            
                            if qvs == 0:
                                # store in statements ftr collection 
                                self.ftr_s.insert_one( classifications | meta )
                                # self.print_classifications( classifications | meta , "statements collection")
                                logging.info(f'Classified -- statement -- on progression counter {progression}')
                            
                            elif qvs == 1:
                                # store in questions ftr collection      
                                self.ftr_q.insert_one( classifications | meta )
                                # self.print_classifications( classifications | meta , "questions collection")
                                logging.info(f'Classified -- question -- on progression counter {progression}')
                        
                        except:
                            logging.info(f"Error sending to DB on sentence number: {progression}")

                    except:
                        logging.info(f"Error classifying sentence on sentence number: {progression}")
                    
                    self.store_counter(progression) # store progression only if last checkpoint value is reached
                
                progression += 1 # increment progression for each iteration
                
                if progression%1000==0:
                    logging.info(f'Progression(%1000==0): {progression} sentences processed')
            
            ### end for

if __name__ == "__main__":
    p = BlogPipeline()
    p.run_pipeline()

