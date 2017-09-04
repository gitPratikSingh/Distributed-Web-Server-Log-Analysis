"""
This module explores the data to find
1. What is the average size of the content returned by the web server
2. What is the min size of the content returned by the web server
3. 1. What is the max size of the content returned by the web server
"""

""""
This module expects an RDD as input. It prints out the content statistics and saves the transformed RDD which
contains all the contentsizes returned by the web server as the elements. This module is called from the main 
after the completion of parsing step
"""

def getContentSizeStats(accessLogs):

  """apply tarnsformation to get the contentsize"""
  ContentSizes=accessLogs.map(lambda x:x.content_size).cache()
  
  """ prints the statistics and saves the RDD in the ContentSizes.txt for latter use by matplotlib"""
  print 'Content size Avg: %d, min: %d, max: %d' %(contentSizes.map(lambda x,y:x+y)/ContentSizes.count(), ContentSizes.min(), ContentSizes.max())
  ContentSizes.saveAsTextFile('hdfs://my-node:9000/Distributed-Web-Server-Analysis/ContentSizes.txt')
  
  
  
