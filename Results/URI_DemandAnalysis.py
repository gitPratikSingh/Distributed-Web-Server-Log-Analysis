
"""
This module takes the parsed RDD as the only input and performs RDD transformations to get the total number 
of request count of all the URIs recorded by the web server via the log. A file named 'URI_DemandAnalysis.txt' 
is saved for latter analysis
"""

def URIDemandAnalysis(accessLogs):
  """ Create a URI,count tuple RDD"""
  URICount=(accessLogs
                      .map(lambda log:(log.endpoint,1))
                      .reduceByKey(lambda x,y:x+y)
                      .cache())
                      
  for URI, count in URICount.takeOrdered(lambda x:-x[1]):
    print 'URI %s : Count %d' %(URI, count)
  
  """Save the RDD in a file"""
  responseCodeToCount.saveAsTextFile('hdfs://my-node:9000/URI_DemandAnalysis.txt')
  
  
