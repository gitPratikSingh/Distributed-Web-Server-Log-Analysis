
"""
This module takes the parsed RDD as the only input and performs RDD transformations to get the average count of all the 
different hosts recorded by the web server in the log in any hour of any day. A file named 'UniqueHostsDaily.txt' is 
saved for future analysis
"""

def UniqueHostsDaily(accessLogs):
  """ Create a date,host tuple RDD """
  dateHostTupleRdd = (accessLogs
                      .map(lambda log:(log.datetime.date(),log.host))
                      .cache())
  """ Group the hosts with the same date """"
  dateGroupedHost = (dateHostTupleRdd.groupByKey().cache())
  
  """ Count the distinct hosts with the same date"""
  dateHostCountPairRdd = (dateGroupedHost.map(lambda x:(x[0], len(set(x[1])))).cache())
  
  for day, uniquehostcount in dateHostCountPairRdd.takeOrdered(20, lambda x:-x[1]):
    print 'Day %s : Count %d' %(day, uniquehostcount)
  
  """Save the RDD in a file"""
  dateHostCountPairRdd.saveAsTextFile('hdfs://my-node:9000/UniqueHostsDaily.txt')
  
