from transformers import pipeline
import pymongo
import langid
import re


class DetectPipeline:

    def __init__(self, split_pattern="(?<!\w\.\w.)(?<![A-Z][a-z]\.)(?<=\.|\?)\s"):
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
        rereplace = [r"\n", r"\r", r"RT", r"(https?.\S+)", r"\<(.*?)\>",'[^\x00-\x7F]']
        self.regex = re.compile('|'.join(rereplace))
        self.split_pattern = split_pattern

    def classifysentence(self, sentence: str):
        svs = self.statementvssentence(sentence)
        fut = self.futurebert(sentence)
        if fut[0]["label"] == "LABEL_1":
            em = self.emotionbert(sentence, top_k=2)
            top = self.topicbert(sentence, top_k=2)
            output = {"text": sentence,
                      "emotion": [em[0]["label"], em[1]["label"]] if em[0]["score"] - em[0]["score"] < 0.1 else [
                          em[0]["label"]],
                      "topic": [top[0]["label"], top[1]["label"]] if top[0]["score"] - top[0]["score"] < 0.1 else [
                          top[0]["label"]]}
            if svs[0]["label"] == "LABEL_0":
                return 0, output
            return 1, output
        return 2, None

    def detect_lang(self, text, lang="en") -> bool:
        return langid.classify(text)[0] == lang

    def split(self, text) -> list:
        text = re.sub(self.regex,"", text)
        text = re.sub(r"\s+", ' ',text)
        return re.split(self.split_pattern, text)

    def findwhitespac(sef, text) -> int:
        return len(re.findall(r'\w+', text))

    def to_database(self, questionlist, statementlist):
        if len(statementlist) > 0:
            self.ftr_s.insert_many(statementlist)
        if len(questionlist) > 1:
            self.ftr_q.insert_many(questionlist)

    def datata_classify(self, data):
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
            self.to_database(questlist, statementlist)
