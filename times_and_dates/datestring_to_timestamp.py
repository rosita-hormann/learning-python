import time
import datetime

s = "2018-11-05 14:39:43"
format = "%Y-%m-%d %H:%M:%S"

print(time.mktime(datetime.datetime.strptime(s, format).timetuple()))