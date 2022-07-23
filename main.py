from detect_pipeline import DetectPipeline
import datetime

str = "I like chicken and i like you. RT I will probably go to tom later this day http://ghjhjk."
meta = {"meta":{"source": "reddit", "time": datetime.datetime.now()}}
# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    dp = DetectPipeline()
    dp.datata_classify(str, meta)
# See PyCharm help at https://www.jetbrains.com/help/pycharm/
