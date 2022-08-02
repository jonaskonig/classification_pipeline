from transformers import pipeline
import pymongo
import langid
import re
import sys
import tensorflow as tf
# add to the top of your code under import tensorflow as tf
tf.config.experimental.enable_tensor_float_32_execution(False)

class DetectPipeline:

    def __init__(self, split_pattern="(?<!\w\.\w.)(?<![A-Z][a-z]\.)(?<=\.|\?)\s", topic_emotion_threshold=0.1,
                 max_label_count=4, min_topic_certainty=0.5, min_emotion_certainty=0.2, stat_file="stats.txt"):
        if sys.version_info[1] < 9 and sys.version_info[0] < 3:
            raise Exception("Must be using Python 3.9 or higher")
        self.CONNNECTSTRING = "***REMOVED***"
        self.topicbert = pipeline("text-classification", model="jonaskoenig/topic_classification_04",
                                  tokenizer="jonaskoenig/topic_classification_04")
        self.emotionbert = pipeline("text-classification", model="jonaskoenig/xtremedistil-l6-h256-uncased-go-emotion",
                                    tokenizer="jonaskoenig/xtremedistil-l6-h256-uncased-go-emotion")
        self.statementvssentence = pipeline("text-classification",
                                            model="jonaskoenig/xtremedistil-l6-h256-uncased-question-vs-statement-classifier",
                                            tokenizer="jonaskoenig/xtremedistil-l6-h256-uncased-question-vs-statement-classifier",
                                            binary_output=True)
        self.futurebert = pipeline("text-classification",
                                   model="jonaskoenig/xtremedistil-l6-h256-uncased-future-time-references-D1",
                                   tokenizer="jonaskoenig/xtremedistil-l6-h256-uncased-future-time-references-D1",
                                   binary_output=True)
        self.myclient = pymongo.MongoClient(self.CONNNECTSTRING)
        self.future = self.myclient["ftr"]
        self.ftr_s = self.future["Future_time_references"]
        self.ftr_q = self.future["Future_time_references_questions"]
        rereplace = [r"\n", r"\r", r"RT", r"(https?.\S+)", r"\<(.*?)\>", '[^\x00-\x7F]']
        self.regex = re.compile('|'.join(rereplace))
        self.split_pattern = split_pattern
        self.topic_emotion_thresh = topic_emotion_threshold
        self.max_label_count = max_label_count
        self.min_topic_certainty = min_topic_certainty
        self.min_emotion_certainty = min_emotion_certainty
        self.stat_file = stat_file

    def classifysentence(self, sentence: str):
        svs = self.statementvssentence(sentence)
        fut = self.futurebert(sentence)
        if fut[0]["label"] == "LABEL_1":
            em = self.emotionbert(sentence, top_k=self.max_label_count)
            top = self.topicbert(sentence, top_k=self.max_label_count)
            if top[0]["score"] < self.min_topic_certainty or em[0]["score"] < self.min_emotion_certainty:
                return 2, None
            topl = [x["label"] for x in top if top[0]["score"] - x["score"] < self.topic_emotion_thresh]
            eml = [x["label"] for x in em if em[0]["score"] - x["score"] < self.topic_emotion_thresh]
            em = [x["score"] for x in em]
            top = [x["score"] for x in top]
            output = {"text": sentence,
                      "emotion": eml,
                      "topic": topl,
                      "emotion_certainty": em[:len(eml)],
                      "topic_certainty": top[:len(topl)]
                      }
            if svs[0]["label"] == "LABEL_0":
                return 0, output
            return 1, output
        return 2, None

    def detect_lang(self, text, lang="en") -> bool:
        return langid.classify(text)[0] == lang

    def split(self, text) -> list:
        split = re.split(self.split_pattern, text)
        split = [re.sub(self.regex, "", s) for s in split]
        split = [ re.sub(r"\s+", ' ', s) for s in split]
        return split

    def findwhitespac(sef, text) -> int:
        return len(re.findall(r'\w+', text))

    def to_database(self, questionlist, statementlist):
        if len(statementlist) > 0:
            self.ftr_s.insert_many(statementlist)
        if len(questionlist) > 0:
            self.ftr_q.insert_many(questionlist)

    def datata_classify(self, data, meta: dict):
        if self.detect_lang(data):
            sentences = self.split(data)
            cleansentences = [x for x in sentences if 4 < self.findwhitespac(x) < 35]
            questlist = []
            statementlist = []
            for sent in cleansentences:
                q, out = self.classifysentence(sent)
                if q == 0:
                    statementlist.append(out)
                if q == 1:
                    questlist.append(out)
            questlist = [x | meta for x in questlist]
            statementlist = [x | meta for x in statementlist]
            self.to_database(questlist, statementlist)
            file1 = open(self.stat_file, "a")  # append mode
            file1.write(f"{len(sentences)},{len(statementlist)},{len(questlist)}\n")
            file1.close()

    def rename_stat_file(self, id: str):
        self.stat_file = f'stats_{id}.txt'
