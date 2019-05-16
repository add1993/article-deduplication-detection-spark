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
from pymongo import MongoClient 
import json

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

def jaccardSim(a,b):
    a1 = set(a)
    b1 = set(b)
    return len(a1 & b1)/len(a1 | b1)

def checkSimilarity(d1, d2):
    global file
    m1 = jaccardSim(d1['PROPN'].split(","), d2['PROPN'].split(","))
    l1p = len(d1['PROPN'])
    l2p = len(d2['PROPN'])
    file.write("\nJaccard similarity for PROPN: "+ str(m1)+"\n")
    file.write(d1['PROPN'])
    file.write(d2['PROPN'])
    print("Jaccard similarity for PROPN: "+ str(m1))

    if( m1 >= 0.7):
        l1v = len(d1['VERB'])
        l2v = len(d2['VERB'])
        m2 = jaccardSim(d1['VERB'].split(","), d2['VERB'].split(","))
        print("Jaccard similarity for VERB: "+str(m2))
        file.write("\nJaccard similarity for VERB: "+str(m2)+"\n")
        file.write(d1['VERB'])
        file.write(d2['VERB'])
        if( m2 > 0.7):
            simi = m1*(1.0)*min(l1p,l2p)/(l1p + l2p) + m2*(1.0)*(min(l2v,l1v))/(l1v + l2v)
            file.write("\nFinal simi score = "+str(simi)+"\n")
            return simi
        else:
            return m2
    else:
        return m1
        
def preprocess(text):
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
            if (similarityScore is not None and similarityScore > 0.8):
                print("\nDuplicate found \n")
                print(articleDict['url'])
                print("stored data \n")
                print(record['url'])
                file.write("\n\nSimilarity percentage: " + str(similarityScore) + "\n")
                file.write("Duplicate found \n")
                file.write("Current article \n")
                file.write(articleDict['url'])
                file.write(articleDict['text'])
                file.write("\n Stored record \n")
                file.write(record['url'])
                file.write(record['text'])
                
                

                isDuplicate = True
                numDup += 1
                #break
    
    if (not isDuplicate):
        rec_id1 = collection.insert(articleDict)
        
    print("total number of files streamed:",count)
    print("the number of similar documents found :",numDup)
    
ssc.start()
ssc.awaitTermination()
