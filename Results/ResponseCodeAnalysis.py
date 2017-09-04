
"""
This module takes the parsed RDD as the only input and performs RDD transformations
to get the count of all the different response codes recorded by the web server in the log.
A file named 'ResponseCodeAnalysis.txt' is saved for latter analysis
"""

def responseCodeAnalysis(accessLogs):
  """ Create a responseCode,count tuple RDD"""
  responseCodeToCount=(accessLogs
                      .map(lambda x:(x.response_code,1))
                      .reduceByKey(lambda x,y:x+y)
                      .cache())
                      
  for responseCode, count in responseCodeToCount.collect():
    print 'Response Code %s : Count %d' %(responseCode, count)
  
  """Save the RDD in a file"""
  responseCodeToCount.saveAsTextFile('hdfs://my-node:9000/ResponseCodeAnalysis.txt')
  
  
  
