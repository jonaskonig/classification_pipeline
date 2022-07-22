from detect_pipeline import DetectPipeline

str = "I like chicken and i like you. RT I will probably go to tom later this day http://ghjhjk."
# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    dp = DetectPipeline()
    dp.datata_classify(str)
# See PyCharm help at https://www.jetbrains.com/help/pycharm/
