# vim: set noexpandtab
import json
import sys

total = 0
for arg in sys.argv[1:]:
	l = len(json.load(open(arg, 'r')))
	total += l
	print("{}\t: {:d}".format(arg, l))

print("total\t: {:d}".format(total))
