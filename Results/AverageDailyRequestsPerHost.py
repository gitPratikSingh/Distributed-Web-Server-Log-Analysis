
"""
This module takes the parsed RDD as the only input and performs RDD transformations to get the average number 
of daily requests received from any hosts. A file named 'AverageDailyRequestsPerHost.txt' is saved for future analysis
"""

def AverageDailyRequestsPerHost(accessLogs):
  """ Create a date,host tuple RDD """
  dateHostTupleRdd = (accessLogs
                      .map(lambda log:(log.datetime.date(),log.host))
                      .cache())
  """ Group the hosts with the same date """"
  dateGroupedHost = (dateHostTupleRdd.groupByKey().cache())
  
  """ Count the distinct hosts with the same date"""
  sortedByDateRdd = (dateGroupedHost.sortBy(lambda x:-x[1])).cache())
  
  for host, requestCount in sortedByDateRdd.takeOrdered(20, lambda x:-x[1]):
    print 'Hosts %s : Request Count %d' %(host, requestCount)
  
  """Save the RDD in a file"""
  sortedByDateRdd.saveAsTextFile('hdfs://my-node:9000/AverageDailyRequestsPerHost.txt')
 
