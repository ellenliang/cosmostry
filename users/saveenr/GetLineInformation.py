
from datetime import datetime

tstart = datetime.now()

line_count = 0
max_line = ""
max_line_size_bytes = 0
max_line_size_chars = 0
max_line_index = 0
with open( r"E:\Mozilla-Bugzilla-Public-02-December-2013.sql\Mozilla-Bugzilla-Public-02-December-2013.sql") as f:
    for line in f:
		if (len(line)>max_line_size_bytes) :
			max_line = line
			max_line_size_bytes = len(line)
			max_line_index = line_count
			u = unicode(line, "utf-8")
			max_line_size_chars = len(u)
			
		line_count = line_count + 1
tend = datetime.now()
rduration = tend - tstart

print "Line count", line_count
print "Max line length in chars", max_line_size_chars
print "Max line length in bytes", max_line_size_bytes
print "Max line index", max_line_index
print "Total time", rduration.seconds

