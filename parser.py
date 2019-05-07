#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun May  5 11:32:40 2019

@author: jitengirdhar
"""
import sys
import pandas as pd
import numpy as np
from ufal.udpipe import Model, Pipeline, ProcessingError # pylint: disable=no-name-in-module
from io import StringIO


#mod = Model.load('spanish-ancora-ud-2.3-181115.udpipe')
mod = Model.load('english-ewt-ud-2.3-181115.udpipe')
#model = mod.load('spanish-ancora-ud-2.3-181115.udpipe')
#sentences = mod.newTokenizer("Hola como estas;Hola como")
pipeline = Pipeline(mod, "tokenizer", Pipeline.DEFAULT, Pipeline.DEFAULT, "conllu")
error = ProcessingError()

# Read whole input
text = ("My name is Jiten Girdhar. I live in USA. India China. Ayush")

# Process data
processed = pipeline.process(text, error)
#print(type(processed))
splitArr = processed.splitlines()
refinedStr = ""
for val in splitArr:
    #print("string -> "+val)
    if (len(val) > 0 and val[0] == '#'):
        continue
    refinedStr += val+'\n'
    
processed = """
1       My      my      PRON    PRP$    Number=Sing|Person=1|Poss=Yes|PronType=Prs      2       nmod:poss       _       _
2       name    name    NOUN    NN      Number=Sing     4       nsubj   _       _
3       is      be      AUX     VBZ     Mood=Ind|Number=Sing|Person=3|Tense=Pres|VerbForm=Fin   4       cop     _       _
4       Jiten   Jiten   PROPN   NNP     Number=Sing     0       root    _       _
5       Girdhar Girdhar PROPN   NNP     Number=Sing     4       flat    _       SpacesAfter=\n"""
data = StringIO(refinedStr)
df = pd.read_csv(data, sep='\t', header=None, names=["idx", "words", "processed_words", "word_type", "col_4", "col_5", "col_6", "col_7", "col_8", "col_9"])
df.to_csv('df.csv')
print(df)
word_type = df["word_type"].unique()
print(word_type)
for type in word_type:
    tmp = df.loc[df['word_type'] == type]
    print("Type = "+type)
    print(tmp["words"])
#print(processed[12]+processed[13]+processed[14]+processed[15] + processed[16] + processed[17])

#for s in sentences:
#    print(s.getText())
#    mod.tag(s)
#    mod.parse(s)
#    print(mod.parse(s))