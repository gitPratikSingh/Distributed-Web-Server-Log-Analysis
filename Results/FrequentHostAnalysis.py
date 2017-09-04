"""
This module explores the data to find the most frequent hosts 
"""

""""
This module expects an RDD as input. It prints out the content statistics and saves the transformed RDD which
contains all the tuples of hosts and its request count as the elements. This module is called from the main 
after the completion of parsing step
"""

def frequentHost(accessLogs): 
  frequentHosts=(accessLogs
                .map(lambda x:(x.host,1))
                .reduceByKey(lambda x,y:x+y)
                .cache())
  print 'The top 20 most frequent hosts are %s,total URI requests sent: %d' %()
  frequentHosts.saveAsTextFile('hdfs://my-node:9000/Distributed-Web-Server-Analysis/FrequentHostAnalysis.txt')
