import re, datetime
from pyspark.sql import Row

"""Parser uses this regular exression to parse a logline into content fields"""
APACHE_ACCESS_LOG_PATTERN = '^(\S+) (\S+) (\S+) \[([\w/:]+\s[+/-]\d{4})] \"(\S+) (\S+)(\s\S*)*\" (\d{3}) (\S+)'

""" """
def parse_apache_time(s):
  month_map = {'Jan': 1, 'Feb': 2, 'Mar':3, 'Apr':4, 'May':5, 'Jun':6, 'Jul':7, 'Aug':8,  'Sep': 9, 'Oct':10, 'Nov': 11, 'Dec': 12}
  return datetime.datetime(int(s[7:11]), month_map[s[3:6]], int(s[0:2]), int(s[12:14]), int(s[15:17]), int(s[18:20]))

def apacheParseLogLine(line):
	match=re.search(APACHE_ACCESS_LOG_PATTERN, line)
	if match is None:
		return (line, 0)
	else:
		size_field=match.group(9)
	
	if size_field=='-':
		size=long(0)
	else:
		size=long(size_field)

	return (Row(
		host=match.group(1),
		clientIdenId=match.group(2),
		userdId=match.group(3),
		datetime=parse_apache_time(match.group(4)),
		method=match.group(5),
		endpoint=match.group(6),
		protocol=match.group(7),
		response_code=match.group(8),
		content_size=size
		), 1)

