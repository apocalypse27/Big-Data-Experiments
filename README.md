# Big-Data-Experiments
All these programs were done as part of my Big Data Coursework.<br>
Note: For all these programs, data is too large to be uploaded
## Hadoop Map-Reduce
#### 1. Mutual Friends
Map Reduce program to find mutual friend list between two friends.
* Data is of the form: P tab F1,F2,F3.....
#### 2. Top 10 friend pairs 
Map reduce program to find top-10 friend pairs with maximum number of mutual friends.
* Same data as the first program is used.
* Program uses the method of job-chaining.
#### 3. Find mean and variance of a large set of numbers
* Employs a combiner to reduce the load off the reducer.
#### 4. Find min,max and median of a large set of numbers
* Employs a custom-partitioner to differentiate between the natural-key and sorting-key.
#### 5. Find resultant matrix of multiplication of two huge sparse matrices
* Each row in data is of the form: <br>
A, 0, 172, 5 <br>
1. Here A is the matrix to which the row belongs.
2. 0 is the row num. <br>
3. 172 is the column num <br>
4. 5 is the value at A[0][172] <br>
* Employs the method of job-chaining.
* First job is that of multiplication and second job is that of addition- the two operations involved in matrix multiplication.

## Spark
All Spark programs are done in Databricks.
#### 1. Find mutual friends
* Same as the first program under Hadoop Map Reduce but done in Spark.
#### 2. Top 10 friends
* Data is same as the above program. In addition, a separate file gives out details of each person of the form <br>
column1: userid <br>
column2: firstname <br>
column3: lastname <br>
column4: address <br>
column5: city <br>
column6: state <br>
column7: zipcode<br>
column8: country<br>
column9: username<br>
column10: dateofbirth<br>

* The program gives top 10 friend pairs with maximum number of mutual friends along with all details of each of the person in that pair.
#### 3. Performing operations on movies data
##### Data
1. movies.csv
Contains movieID, title and genre
2. ratings.csv
Contains userID, movieID, ratings and timestamp
3. tags.csv

##### Code to perform the following
1a. Calculate the average ratings of each movie.
1b. Give the names of bottom 10 movies with lowest average ratings.
2. Find average rating of each movie where the movie’s tag is ‘action’.
3. Find average rating of each movie where the movie’s tag is ‘action’  and  genre contains ‘thrill’.
