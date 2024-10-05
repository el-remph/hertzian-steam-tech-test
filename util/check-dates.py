#!/usr/bin/python3
# vim: set noexpandtab
from datetime import date
import json
import sys

exit_val = 0
min_date, max_date = [date.fromisoformat(arg) for arg in sys.argv[1:3]]
for arg in sys.argv[3:]:
	for review in json.load(open(arg, 'r')):
		datestr = review['date']
		if not min_date <= date.fromisoformat(datestr) <= max_date:
			print("{}: Bad date: {}".format(arg, datestr))
			exit_val = 1

sys.exit(exit_val)
