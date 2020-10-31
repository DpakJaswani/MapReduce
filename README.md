# MapReduce

# Question-1 
```
Write a MapReduce program in Hadoop that implements a simple “Mutual/Common friend list of two friends". The key idea is that if two people are friend then they have a lot of mutual/common friends. This program will find the common/mutual friend list for them.

For example,
Alice’s friends are Bob, Sam, Sara, Nancy
Bob’s friends are Alice, Sam, Clara, Nancy
Sara’s friends are Alice, Sam, Clara, Nancy

As Alice and Bob are friend and so, their mutual friend list is [Sam, Nancy]
As Sara and Bob are not friend and so, their mutual friend list is empty. (In this case you may exclude them from your output). 

Output: The output should contain one line per user in the following format:
<User_A>, <User_B><TAB><Mutual/Common Friend List>
```
# Question-2
```
Please answer this question by using dataset from Q1.
Find friend pair(s) whose number of common friends is the maximum in all the pairs. 

Output Format:
<User_A>, <User_B><TAB><Mutual/Common Friend Number>
```
# Question-3
```
Please use in-memory join at the Mapper to answer the following question.
Given any two Users (they are friend) as input, output the list of the names and the date of birth (mm/dd/yyyy) of their mutual friends.

Note that the userdata.txt will be used to get the extra user information and cached/replicated at each mapper.

Output format:
UserA id, UserB id, list of [names: date of birth (mm/dd/yyyy)] of their mutual Friends.

Sample Output:
1234     4312       [John:12/05/1985, Jane : 10/04/1983, Ted: 08/06/1982]
```
# Question-4
```
Please use in-memory join at the Reducer to answer the following question.

For each user print User ID and maximum age of direct friends of this user.

Sample output:
  
User A, 60
where User A is the id of a user and 60 represents the maximum age of direct friends for this particular user.
```
# Input Data Files Format
```
1. soc-LiveJournal1Adj.txt
The input contains the adjacency list and has multiple lines in the following format:
<User><TAB><Friends>
Hence, each line represents a particular user’s friend list separated by comma.
2. userdata.txt 
The userdata.txt contains dummy data which consist of 
column1 : userid
column2 : firstname
column3 : lastname
column4 : address
column5: city
column6 :state
column7 : zipcode
column8 :country
column9 :username
column10 : date of birth.

Here, <User> is a unique integer ID corresponding to a unique user and <Friends> is a comma-separated list of unique IDs corresponding to the friends of the user with the unique ID <User>. Note that the friendships are mutual (i.e., edges are undirected): if A is friend with B then B is also friend with A. The data provided is consistent with that rule as there is an explicit entry for each side of each edge. So when you make the pair, always consider (A, B) or (B, A) for user A and B but not both.
```

# How to Run:
```
Step-1 : put soc-LiveJournal1Adj.txt  and userdata.txt file in hdfs
hdfs dfs -put <file path in your system> <file path where you want to store in hdfs>

To run the programs follow these commands

Question-1
hadoop jar <(path to) assignment1.jar> MutualFriends <hdfs path of soc-LiveJournal1Adj.txt> <output directory>

Question-2
hadoop jar <path to assignment1.jar> MutualFriendsTop <hdfs path of soc-LiveJournal1Adj.txt> <output directory>

Question-3
put friendsPair file(s) in hadoop by following the command mentioned in step-1 
File format for friendspair file is : have two UserIDs comma(,) separated
I have already included 3 of these files in the folder

hadoop jar <(path to)assignment1.jar> InMemoryJoin <hdfs path of friendspair file> <hdfs path of soc-LiveJournal1Adj.txt> <hdfs path of userdata.txt> <output directory>

Question-4
hadoop jar <(path to)assignment1.jar> RsJoin <hdfs path of soc-LiveJournal1Adj.txt> <hdfs path of userdata.txt> <output directory>
```
