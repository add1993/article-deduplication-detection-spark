#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue May  7 18:02:07 2019

@author: jitengirdhar
"""

import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.ml import Pipeline
from pyspark.ml import PipelineModel
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import HashingTF, Tokenizer
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit
from kafka import KafkaConsumer
import pandas as pd
import numpy as np
from ufal.udpipe import Model, Pipeline, ProcessingError # pylint: disable=no-name-in-module
from io import StringIO
from scipy.spatial import distance
from newsplease import NewsPlease
from pymongo import MongoClient 
import json
from bson import json_util

print("Starting streaming program")
sc = SparkContext("local[2]", "StreamData")
sqlContext = SQLContext(sc)
ssc = StreamingContext(sc, 1)
file = open("streamlog.txt", "a")
try: 
    conn = MongoClient() 
    print("Connected successfully!!!") 
except: 
    print("Could not connect to MongoDB") 

db = conn.Deduplication
collection = db.deduplication_collection 
consumer = KafkaConsumer('test', bootstrap_servers = ['localhost:9092'])

mod = Model.load('english-ewt-ud-2.3-181115.udpipe')
pipeline = Pipeline(mod, "tokenizer", Pipeline.DEFAULT, Pipeline.DEFAULT, "conllu")
error = ProcessingError()

def getString(array):
    result = ""
    for i in range(array.size):
        result += str(array[i]) + ","
        
    return result

def checkSimilarity(d1,d2):
    m1 = (1.0 - distance.jaccard(d1['PROPN'], d2['PROPN']))
    print("Jaccard similarity for PROPN: "+ str(m1))
    if( m1 >= 0.5):
        m2 = 1.0 - distance.jaccard(d1['VERB'], d2['VERB'])
        print("Jaccard similarity for VERB: "+str(m2))
        if( m2 > 0.5):
            return min(1,m1+m2)

        else:
            return m1
        
def preprocess(text):
    print("starting processing")
    processed = pipeline.process(text, error)
    print("processing")
    splitArr = processed.splitlines()
    refinedStr = ""
    for val in splitArr:
        if (len(val) > 0 and val[0] == '#'):
            continue
        refinedStr += val+'\n'
        
    data = StringIO(refinedStr)
    df = pd.read_csv(data, sep='\t', header=None, names=["idx", "words", "processed_words", "word_type", "col_4", "col_5", "col_6", "col_7", "col_8", "col_9"])
    word_type = df["word_type"].unique()
    textDet= {}
    for type in word_type:
        tmp = df.loc[df['word_type'] == type]
        listStr = getString(np.array(tmp["words"]))
        textDet[type] = listStr
    return textDet

if __name__== "__main__":  
    count = 0
    numDup = 0    
    for text in consumer:
        data = ""
        data = str(text.value, 'utf-8')
        if data == "":
            continue
        
        try:
            articleDict = json.loads(data)
        except ValueError as e:
            print("Invalid json found")
            continue
        
        dataDict = preprocess(articleDict['text'])
        articleDict['udpipe_data'] = dataDict
        jsonData = json.dumps(articleDict, indent=4,  sort_keys=True, default=str)
        count += 1
        cursor = collection.find() 
        isDuplicate = False
        for record in cursor:
            if 'udpipe_data' in record.keys():
                storedDict = record['udpipe_data']
                similarityScore = checkSimilarity(dataDict, storedDict)
                print("Similarity Score "+str(similarityScore))
                if (similarityScore is not None and similarityScore >= 0.5):
                    print("Duplicate found")
                    isDuplicate = True
                    numDup += 1
                    break
        
        if (not isDuplicate):
            rec_id1 = collection.insert(articleDict) 
        print("total number of files streamed:",count)
        print("the number of similar documents found :",numDup)
        file.write("Data received"+"\n")
        file.write(data+"\n")
        file.write("Prediction  |  Label\n")
        
    ssc.start()
    ssc.awaitTermination()
