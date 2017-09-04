
"""
This module takes the parsed RDD as the only input and performs RDD transformations
to get the count of all the error response codes recorded by the web server in the log.
A file named 'ErrorResponseCodeAnalysis.txt' is saved for latter analysis
"""

def ErrorResponseCodeAnalysis(accessLogs):
  """ Create a errorResponseCode,count tuple RDD"""
  errorResponseCodeToCount=(accessLogs
                            .filter(lambda log:log.response_code!=200)
                            .map(lambda log:(log.endpoint,1))
                            .reduceByKey(lambda x,y:x+y)
                            .cache())
                      
  for errorResponseCode, count in errorResponseCodeToCount.collect():
    print 'Error Response Code %s : Count %d' %(errorResponseCode, count)
  
  """Save the RDD in a file"""
  errorResponseCodeToCount.saveAsTextFile('hdfs://my-node:9000/ErrorResponseCodeAnalysis.txt')
