#!/bin/sh
# To be run on the output 5000-review json files. If nothing is printed,
# all is well
set -o pipefail
grep -h '^[[:space:]]*"id":' "$@" | sort | uniq -cd
grep -h '^[[:space:]]*"date":' "$@" | sort -cr
