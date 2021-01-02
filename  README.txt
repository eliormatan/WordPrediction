Knowledge-base for Word Prediction


Elior Matan : 308470715        eliorma@post.bgu.ac.il
Ibrahim Awad : 318260585        awadi@post.bgu.ac.il


How to run: create jars for each source file and upload them to a predefined bucket in S3 “dsp-211-ass2”.
Run the command: “java -jar WordPrediction.jar”


Implementation:


Main:
Create all the steps for our program flow and provided jar and input/output locations on s3, and run them using run job flow.


Step1 - Divide:
The first step of our workflow is to divide the corpus into two parts, the training data, and the held out data.
Mapper: takes as input the hebrew 3gram corpus, and divides it into two parts based on even/odd lines. Each line represents the 3gram and its occurrences in each part of the corpus.
Mapper input Value: [3gram year occurrences pages books]
Mapper output: Key = 3gram                Value = r1:0 / 0:r2
Reducer: Sums all occurences of the given ngram (the key) on each part of the corpus.
Reducer output: Key = 3gram                Value = r1        r2


Step2 - CalcProb:
Calculate N, Nr, Tr, for both sides of the corpus, as well as probability for each r found.
Mapper input Value = “3gram r1 r2”.
Mapper output: Key = r1+r2.                Value = “r1/r2 1”
Reducer output: Key: r        Value: probability


Step3 - JoinResults:
Join the 3grams with their probabilities into one file, we do this by combining Step1 and Step2 outputs based on r as a foreign key, using a mapper for each input file.
Mapper 1 input value: “3gram r1 r2”
Mapper 1 output: Key = r1+r2:2                Value = 3gram
Mapper 2 input value: “r probability”
Mapper 2 output: Key = r:1        Value = probability
Reducer input: Key = r:1/2        Value = prob / 3gram
Reducer output: Key = 3gram                Value = probability
In this step we used a partitioner with r as key, and relied on hadoop sorting to get the probability before the corresponding 3grams.




Step4 - Sort:
Sort the 3gram-probability results as instructed in the assignment: (1) by w1w2, ascending; (2) by the probability for w1w2w3, descending.
Mapper input Value: “3gram=w1w2w3        probability=p”
Mapper output: Key = w1w2 (1-p)        Value = “3gram=w1w2w3        probability=p”
Reducer output: Key = 3gram                Value = probability
For sorting the results we relied on hadoop’s auto-sorting between the mapper and reducer, based on the first two words which are sorted ascendingly, and to sort the probabilities descendingly we used a neat trick: used the probabilities complement, which sorted the keys as required.


Output :
https://dsp-211-ass2.s3.amazonaws.com/sortResultsOut/part-r-00000