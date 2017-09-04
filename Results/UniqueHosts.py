"""
This module takes the parsed RDD as the only input and performs RDD transformations
to get the count of distinct hosts recorded by the web server in the log.
A file named 'UniqueHosts.txt' is saved for latter analysis
"""

def UniqueHosts(accessLogs):
  """ Create a uniqueHosts RDD"""
  uniqueHostsRdd=(accessLogs
                  .map(lambda log:log.host)
                  .cache())
                      
  for count in uniqueHostsRdd.distinct().count():
    print 'Total Number of Unique Hosts : Count %d' %(count)
  
  """Save the RDD in a file"""
  uniqueHostsRdd.distinct().saveAsTextFile('hdfs://my-node:9000/UniqueHosts.txt')
