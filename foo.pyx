#!/usr/bin/python3
# vim: noexpandtab:ts=8:shiftwidth=8:
# cython: language_level=3str
from libc.stdint cimport *

import datetime
import hashlib
import json
import jsonschema
import logging
import operator
import requests

# hex string -- should we store as an integer?
cdef dict[str, str] hex224_schema = {"type": "string", "pattern": "^[A-Fa-f0-9]{56}$"}
# TODO: can we make this schema redundant by defining a review as a type-checked struct?
schema : dict[str, type] = {
	"type": "array",
	"items": {
		"type": "object",
		"properties": {
			"id":	hex224_schema,
			"author":	hex224_schema,
			"date":	{"type": "string", "format": "date"},
			"hours":	{"type": "integer"},
			"content":	{"type": "string"},
			"comments":	{"type": "integer"},
			"source":	{"type": "string", "pattern": "^steam$"},
			"helpful":	{"type": "integer"},
			"funny":	{"type": "integer"},
			"recommended":	{"type": "boolean"}
		}
	}
}

ctypedef dict[str, type] Review

cpdef enum Steam_Date_Type:
	CREATED, UPDATED

# TODO: convert some strings to const memoryviews. Actually convert lots of
# this to memoryviews

cdef str hexdigest224(str s):
	return hashlib.blake2s(s.encode('utf-8'), digest_size=28).hexdigest()

cdef class Review_Stream:
	# Member/attribute declarations
	cdef object steamid, connection
	cdef str filter_field, timestamp
	cdef readonly str cursor
	cdef readonly dict response_obj

	def __init__(self, steamid, const Steam_Date_Type date_type):
		self.steamid = steamid # constant
		self.cursor = '*' # assigned anew on each iteration
		self.connection = requests.Session() # just to reuse the TCP connection

		# Cython doesn't do `match'
		if date_type == Steam_Date_Type.CREATED:
			self.filter_field = 'recent'
			self.timestamp = 'timestamp_created'
		elif date_type == Steam_Date_Type.UPDATED:
			self.filter_field = 'updated'
			self.timestamp = 'timestamp_updated'
		else:
			raise TypeError

	# transforms steam input format review into output format Review. obj is a
	# decoded json dict from the reviews array. This is a pure function and a good
	# place to optimise
	cdef Review xform_review(self, dict obj):
		return {
			'id'		: hexdigest224(obj['recommendationid']),
			'author'	: hexdigest224(obj['author']['steamid']),
			# TODO: UTC? timestamp_updated or timestamp_created?
			'date'		: datetime.date.fromtimestamp(obj[self.timestamp]).isoformat(),
			'hours'		: obj['author']['playtime_at_review'],
			'content'	: obj['review'],
			'comments'	: obj['comment_count'],
			'source'	: 'steam',
			'helpful'	: obj['votes_up'],
			'funny'		: obj['votes_funny'],
			'recommended'	: obj['voted_up'] # apparently
			# TODO: franchise and gameName -- are they really to be stored
			# separately for each review?
		}

	cpdef list[Review] nextbatch(self, const uintmax_t n_max):
		r = self.connection.get("https://store.steampowered.com/appreviews/{:d}".format(self.steamid),
				params={'json':1, 'filter':self.filter_field, 'num_per_page':n_max, 'cursor':self.cursor})
		r.raise_for_status()

		self.response_obj = r.json()
		if not self.response_obj['success']:
			raise Exception('bad response')
		assert self.response_obj['query_summary']['num_reviews'] == len(self.response_obj['reviews'])

		self.cursor = self.response_obj['cursor'] # TODO: send next request asynchronously here, if not eof
		return [self.xform_review(x) for x in self.response_obj['reviews']]

cdef class Split_Reviews:
	cdef void count_id_frequency(self, list[Review] reviews):
		for review in reviews:
			Id = review['id'] # again, I'd rather this be a uint but steam says string
			if Id in self.ids:
				# this is slow because python insists on checking
				# if self.ids is None *every single loop*
				self.ids[Id] += 1
			else:
				self.ids[Id] = 0

	cdef bint getbatch(self):
		if self.eof:
			return False
		cdef list[Review] reviews = self.steam.nextbatch(self.per_file)
		self.total -= len(reviews)
		self.count_id_frequency(reviews)
		self.reviews += reviews
		logging.debug('received {:d} reviews, now have {:d}'.format(len(reviews), len(self.reviews)))
		if len(reviews) == 0:
			self.eof = True
		return not self.eof

	cdef list[Review] sort_reviews(self, const size_t n):
		# Reviews are received already sorted by date (descending), so it
		# would be wasteful to sort by id, then by date again. Instead,
		# pop a contiguous portion of self.reviews with the same date,
		# sort that, repeat until we have n reviews
		cdef list[Review] result = [], same_date
		while len(result) < n:
			same_date = [self.reviews.pop(0)]
			while len(same_date) + len(result) < n \
				and self.reviews[0]['date'] == same_date[0]['date']:
				same_date += [self.reviews.pop(0)]
			same_date.sort(key=operator.itemgetter('id'))
			result += same_date
		return result

	cdef void writebatch(self):
		cdef str outfilename = "{:d}.{:d}.json".format(self.steamid, self.file_i)
		self.file_i += 1 # whatever happend to postincrement?

		cdef uintmax_t writeme = min(self.per_file, len(self.reviews))
		logging.info("Writing {:d} reviews to {}".format(writeme, outfilename))

		cdef list[Review] towrite = self.sort_reviews(writeme)
		with open(outfilename, "wt") as outfile:
			json.dump(towrite, outfile, indent="\t")
		# Validate *after* dumping, so the bad json can still be inspected
		# after crash. Validating only the ones to be written prevents
		# some reviews from being validated multiple times needlessly
		jsonschema.validate(towrite, schema,
				format_checker=jsonschema.Draft202012Validator.FORMAT_CHECKER)
		if self.max_files and self.file_i >= self.max_files:
			assert not self.file_i > self.max_files
			self.eof = True

	cdef object steamid, steam
	cdef list[Review] reviews
	cdef dict[str, uintmax_t] ids
	cdef uintmax_t per_file, max_files
	cdef intmax_t total
	cdef readonly uintmax_t file_i # read by test.py
	cdef bint eof, flushed

	# max_files=0 means unlimited
	def __init__(self, steamid, uintmax_t per_file=5000, uintmax_t max_files=0, Steam_Date_Type date_type=Steam_Date_Type.CREATED):
		self.steamid = steamid	# constant
		self.reviews	= []	# accumulates with each iteration
		self.ids	= {}	# counts frequency of each id (should all be 1) self.ids
		self.total	= 0	# decrements after each iter (set after first as a special case) self.total
		self.file_i	= 0	# incremented monotonically
		self.per_file	= per_file	# constant
		self.max_files	= max_files	# constant
		self.eof	= False
		self.flushed	= False
		self.steam = Review_Stream(steamid, date_type)

		# First request: get total_reviews also. Construction implies making a
		# network request, but not necessarily writing
		self.getbatch()
		self.total = self.steam.response_obj['query_summary']['total_reviews'] - len(self.reviews)


	def loop(self):
		while self.getbatch():
			if len(self.reviews) >= self.per_file:
				self.writebatch()

	def end(self):
		if self.flushed:
			return

		while len(self.reviews):
			self.writebatch()
		self.flushed = True

		if self.total != 0:
			logging.warning('more reviews than expected: {:d}'.format(-self.total))

		for Id, dups in self.ids.items():
			if dups != 0:
				logging.warning('id "{}" has {:d} duplicates'.format(Id, dups))

		logging.debug('final cursor was {}'.format(self.steam.cursor))

	def __del__(self):
		self.end()
