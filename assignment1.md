# Answer 1
## Pairs
In the Pairs implementatoiin, I first performed WordCount which gave me the counts for each word, which i saved in a temporary file.
Then, I took all the words in the temporary file and stored them in a hashmap.
I also took an enum variable to store the counts of lines.
The intermediate values was the Pair along with the total occurences of that pair.
To calculate PMI, i took the sum for each pair from the second mapreduce(pairs) and then the sum of words from the hashmap i created. 
There were 2 mapreduce jobs (1 for wordcount, 1 for pairs implementation)
The input file for wordcount was Shakespeare.txt and for Pairs, it was Shakespeare.txt and the temporary file i made.

## Stripes
In the Stripes Implementation,  I first performed WordCount which gave me the counts for each word, which i saved in a temporary file.
Then, I took all the words in the temporary file and stored them in a hashmap.
I also took an enum variable to store the counts of lines.
The intermediate values were a key and a hashmap. The key was the first element for the pair. The key of the hashmap was the second element of the pair.
The value for the hashmap was the total number of occurrences for the pair.


# Answer 2
## Stripes 
Build Time :37.12s
Run Time:12.94s
Run on Cs environment

## Pairs
Build Time: 37.12s
Run Time:12.98s
Run on Cs environment

# Answer 3
## Stripes
Run Time: 13.74s

## Pairs
Run Time: 14.22s

# Answer 4
32258

# Answer 5 
## Highest PMI
(cleopatra's, alexandria)    (3.5387795, 12)
(alexandria, cleopatra's)    (3.53887795,12)

## Lowest PMI
(not, you)    (-1.9125345, 10)
(you, not)    (-1.9125345, 10)

# Answer 6 (death)
(death, father's)    (0.87721395, 12)
(death, till)    (0.630876, 18)
(death, die)    (0.57806814, 12)
