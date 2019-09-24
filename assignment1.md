Assignment 1
=============
## Question 1
* PairPMI:
    There are two mapreduce jobs, one is called job and another is called job_pair.
    The first job : 
    + The mapper emits a pair of a string and a integer, e.g., emits (word, 1) for every word occurrence.
    + The reducer take what is emitted by the mapper as input, and calculate the sum of the occurence with same key(word).
    + The method FileOutputFormat.setOutputPath sets the path for output files in this step. These files will be removed later.
    The Second job:
    + The mapper emits a pair of words as the key, and the occurence as the value, (Pair_of_Words->key, integer->value) , e.g., (('aaa','bbb'),1).
    + The reducer take what is emitted by the mapper as input, and calculate the sum of the occurence with same key(same pair of words).
    + The reducer calculates the PMI with respect to the equation given on website.
    + The reducer deletes the output files created by first job and write the final result in format (k1,v1) (PMI, co-occurence).

* StripePMI
    There are two mapreduce jobs, one is called job and another is called job_stripe.
    The first job : 
    + The mapper emits a pair of a string and a integer, e.g., emits (word, 1) for every word occurrence.
    + The reducer take what is emitted by the mapper as input, and calculate the sum of the occurence with same key(word).
    + The method FileOutputFormat.setOutputPath sets the path for output files in this step. These files will be removed later.

    The Second job:
    + The mapper emits a pair of words as the key, and the occurence as the value, emit (mainKey,{k1,v1,k2,v2}) etc ('cs651', {"NB",1,'awsl',11...})
    + The reducer take what is emitted by the mapper as input, and calculate the sum of the occurence with same mainKey and key(same pair of words).
    + The reducer calculates the PMI with respect to the equation given on website.
    + The reducer deletes the output files created by first job and write the final result in format (mainKey, {k1:(PMI_1, co_coourence_1), k2 : (PMI_2, co_coourence_2)...} ) .





## Question 2.
Pairs  ubuntu : 25.502s
Stripes ubuntu :  8.519s

## Question 3.
Pairs  ubuntu : 28.519s
Stripes: 9.465s

## Question 4.
  77198  308792 2327632


## Question 5. (highest PMI)
(maine, anjou)	(3.6331422, 12)
(anjou, maine)	(3.6331422, 12)


## Question 5. (lowest PMI)
(thy, you)	(-1.5303968, 11)
(you, thy)	(-1.5303968, 11)


## Question 6. ('tears')
(tears, shed)	(2.1117902, 15)
(tears, salt)	(2.052812, 11)
(tears, eyes)	(1.165167, 23)


## Question 6. ('death')
(death, father's)	(1.120252, 21)
(death, die)	(0.7541594, 18)
(death, life)	(0.73813456, 31)


## Question 7.
(hockey, defenceman)	(2.4180872, 153)
(hockey, winger)	(2.3700917, 188)
(hockey, sledge)	(2.352185, 93)
(hockey, goaltender)	(2.2537384, 199)
(hockey, ice)	(2.2093477, 2160)


## Question 8.
(data, cooling)	(2.0979042, 74)
(data, encryption)	(2.0443723, 53)
(data, array)	(1.9926307, 50)
(data, storage)	(1.9878386, 110)
(data, database)	(1.8893089, 99)