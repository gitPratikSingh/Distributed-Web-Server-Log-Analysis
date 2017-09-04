"""
This module returns three RDDs(Resilient Distributed DataSets) 
1.parsed_logs: Contains all the logs 
2.access_logs: Contains all the logs which matches the regular expression 
3.failed_logs: Contains all the unmatched logs

The returned RDDs are already cached, so it is faster to run any exploratory analytical query on them
"""

import sys
import os

base_dir=os.path.join('/home')
input_file=os.path.join('pratik', 'Downloads', 'NASA_access_log_Jul95')
logfile=os.path.join(base_dir,input_file)

def parseLogs(sc):
	parsed_logs=(sc.textFile(logfile).map(apacheParseLogLine).cache())	
	access_logs=(parsed_logs.filter(lambda x:x[1]==1).map(lambda x:x[0]).cache())
	failed_logs=(parsed_logs.filter(lambda x:x[1]==0).map(lambda x:x[0]).cache())
	failed_logs_count = failed_logs.count()

	if failed_logs_count > 0:
		print 'Number of failed logs %d' % failed_logs_count
		for line in failed_logs.take(20):
			print 'Invalid logline %s' %line

	print 'Read %d lines, successfully parsed %d lines, failed to parse %d lines' %(parsed_logs.count(), access_logs.count(), failed_logs.count())

	return parsed_logs, access_logs, failed_logs
