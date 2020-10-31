# MapReduce
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
