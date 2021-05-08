
How to Run this program:

1  First, importing the Hadoop-required packages.
2. Uploading Batting.csv and People.csv in Hadoop Server in specific path
3. To make a jar file for MRSQL.class
4. Using the following command, to run this jar:

Query 1:
$ hadoop jar <jar file path> MRSQL <your path/Batting.csv> <sum of hr value> <output path>

Query 2:
$ hadoop jar <jar file path> MRSQL <your path/Batting.csv> <your path/People.csv> <year_id> <hr value> <output path>

5.The output will be displayed in the following command.

 Query 1:
 $ hadoop fs -cat <output path with file name>

 Query 2:
 $  hadoop fs -cat <output path with file name>

Note: I have used entire Batting.csv and People.csv dataset and performed the required operation programmatically to call the data for this assignement. 
