"""
This module explores the 404 errors. This error happens due to invalid URLs. This module can be used to get all/top URI NOT FOUND errors.
This module does the following analysis on top of the logs which are associated with these errors:

1. It lists the 404 errors in a file and prints 20lines of such logs in the driver program
2. It lists top 20 404 error end points/URIs
3. It lists the hosts with most number of 404 errors
4. It lists number of 404 error codes per day
5. It lists hourly 404 response codes

"""

def ErrorResponseCodeAnalysis(accessLogs):
  """ Create a errorResponseCode404 log RDD"""
  errorResponseCode404Logs=(accessLogs
                            .filter(lambda log:log.response_code==404)
                            .cache())
                      
  for log in errorResponseCode404Logs.take(20):
    print '404 Error: %s' %(log)
  
  """URIs with most number of 404 errors """
  errorResponseCode404URI=(errorResponseCode404Logs
                           .map(lambda log:(log.endpoint,1))
                           .reduceByKey(lambda x,y:x+y)
                           .cache())
  
  for URI, count in errorResponseCode404URI.takeOrdered(20, lambda x:-x[1]):
    print 'URI : %s, Count : %d' %(URI, count)
  
  """Hosts with most number of 404 errors """
  hostErrorResponseCode404Count=(errorResponseCode404Logs
                           .map(lambda log:(log.host,1))
                           .reduceByKey(lambda x,y:x+y)
                           .cache())
  
  for host, count in hostErrorResponseCode404Count.takeOrdered(20, lambda x:-x[1]):
    print 'URI : %s, Count : %d' %(host, count)
 
  
  """404 errors per day"""
  errorResponseCode404CountperDay=(errorResponseCode404Logs
                                   .map(lambda log:(log.datetime.date(),1))
                                   .reduceByKey(lambda x,y:x+y)
                                   .cache())
  
  for day, count in errorResponseCode404CountperDay.takeOrdered(20, lambda x:-x[1]):
    print 'Day : %s, Count : %d' %(day, count)
 
 
  
  """ hourly 404 errors"""
  errorResponseCode404CountperHour=(errorResponseCode404Logs
                                   .map(lambda log:(log.datetime.hour(),1))
                                   .reduceByKey(lambda x,y:x+y)
                                   .cache())
  
  for hour, count in errorResponseCode404CountperHour.takeOrdered(20, lambda x:-x[1]):
    print 'Hour : %s, Count : %d' %(hour, count)
 

  """Save the RDD in a file"""
  errorResponseCode404Logs.saveAsTextFile('hdfs://my-node:9000/URI_NOT_FOUND_Analysis.txt')
