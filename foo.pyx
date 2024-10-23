#!/usr/bin/python3
# vim: noexpandtab:ts=8:shiftwidth=8:
# cython: language_level=3str
from libc.stdint cimport *
from cython cimport dataclasses

import asyncio
import datetime
import hashlib
import json
import logging
import operator
import requests

cdef str hexdigest224(str s):
	return hashlib.blake2s(s.encode('utf-8'), digest_size=28).hexdigest()

@dataclasses.dataclass(init=False)
cdef class Review:
	# Unfortunately to maintain backwards-compatibility these must be
	# declared in this order
	cdef readonly str id, author, date
	cdef readonly uintmax_t hours
	cdef readonly str content
	cdef readonly uintmax_t comments
	cdef readonly str source
	cdef readonly uintmax_t helpful, funny
	cdef readonly bint recommended

	# transforms steam input format review into output format review. obj is a
	# decoded json dict from the reviews array
	def __init__(self, obj, which_timestamp):
		self.id		= hexdigest224(obj['recommendationid'])
		self.author	= hexdigest224(obj['author']['steamid'])
		self.date	= datetime.date.fromtimestamp(obj[which_timestamp]).isoformat() # TODO= UTC?
		self.hours	= obj['author']['playtime_at_review']
		self.content	= obj['review']
		self.comments	= obj['comment_count']
		self.source	= 'steam'
		self.helpful	= obj['votes_up']
		self.funny	= obj['votes_funny']
		self.recommended	= obj['voted_up'] # apparently
		# TODO: franchise and gameName -- are they really to be stored
		# separately for each review?

# Not pretty
cpdef list[dict[str, type]] reviews_dicts(list[Review] reviews):
	return [{name: r.__getattribute__(name)
		for name in r.__dataclass_fields__.keys()}
			for r in reviews]

cpdef enum Steam_Date_Type:
	CREATED, UPDATED

cdef class Review_Stream:
	# Member/attribute declarations
	cdef object steamid, connection, request
	cdef uintmax_t n_max
	cdef str filter_field, timestamp
	cdef readonly dict response_obj

	async def send_request(self, cursor):
		return self.connection.get("https://store.steampowered.com/appreviews/{:d}".format(self.steamid),
				params={'json':1, 'filter':self.filter_field, 'num_per_page':self.n_max, 'cursor':cursor})

	def __init__(self, steamid, uintmax_t n_max, Steam_Date_Type date_type):
		self.steamid = steamid	# constant
		self.n_max = n_max	# constant
		# Cython doesn't do `match'
		if date_type == Steam_Date_Type.CREATED:
			self.filter_field = 'recent'
			self.timestamp = 'timestamp_created'
		elif date_type == Steam_Date_Type.UPDATED:
			self.filter_field = 'updated'
			self.timestamp = 'timestamp_updated'
		else:
			raise TypeError

		# just to reuse the TCP connection
		self.connection = requests.Session()
		# init first request -- subsequent requests will be sent while looping
		self.request = asyncio.create_task(self.send_request('*'))

	async def nextbatch(self) -> list[Review]:
		r = await self.request
		r.raise_for_status()

		self.response_obj = r.json()
		if not self.response_obj['success']:
			raise Exception('bad response')
		assert self.response_obj['query_summary']['num_reviews'] == len(self.response_obj['reviews'])

		if len(self.response_obj['reviews']) == 0:
			return [] # EOF: rest of function not relevant

		self.request = asyncio.create_task(self.send_request(self.response_obj['cursor']))
		return [Review(x, self.timestamp) for x in self.response_obj['reviews']]


cdef class Split_Reviews:
	cdef void count_id_frequency(self, list[Review] reviews):
		for review in reviews:
			Id = review.id
			if Id in self.ids:
				# this is slow because python insists on checking
				# if self.ids is None *every single loop*
				self.ids[Id] += 1
			else:
				self.ids[Id] = 0

	async def getbatch(self) -> cython.bint:
		if self.eof:
			return False
		reviews = await self.steam.nextbatch()
		self.total -= len(reviews)
		self.count_id_frequency(reviews)
		self.reviews += reviews
		logging.debug('received {:d} reviews, now have {:d}'.format(len(reviews), len(self.reviews)))
		if len(reviews) == 0:
			self.eof = True
		return not self.eof

	cdef void sort_reviews(self, const size_t n):
		# Reviews are received already sorted by date (descending), so it
		# would be wasteful to sort by id, then by date again. Instead,
		# select a contiguous portion of self.reviews with the same date,
		# sort that, repeat until we have n reviews
		cdef size_t begin = 0, end = 1
		while begin + end < n:
			while begin + end < n \
				and self.reviews[end].date == self.reviews[begin].date:
				end += 1
			# TODO: why can't we sort a slice in place?
			self.reviews[begin:end] = \
				sorted(self.reviews[begin:end], key=operator.attrgetter('id'))
			begin = end
			end += 1

	@staticmethod
	async def writebatch_task(reviews, outfilename):
		with open(outfilename, "wt") as outfile:
			json.dump(reviews_dicts(reviews), outfile, indent="\t")

	cdef void writebatch(self, jobs: asyncio.TaskGroup):
		cdef str outfilename = "{:d}.{:d}.json".format(self.steamid, self.file_i)
		self.file_i += 1 # whatever happend to postincrement?

		cdef uintmax_t writeme = min(self.per_file, len(self.reviews))
		logging.info("Writing {:d} reviews to {}".format(writeme, outfilename))

		self.sort_reviews(writeme)
		jobs.create_task(self.writebatch_task(self.reviews[:writeme], outfilename))
		del self.reviews[:writeme]
		if self.max_files and self.file_i >= self.max_files:
			assert not self.file_i > self.max_files
			self.eof = True

	async def main_loop(self):
		self.steam = Review_Stream(self.steamid, self.per_file, self.date_type)
		async with asyncio.TaskGroup() as writejobs:
			try:
				# First request: get total_reviews also
				await self.getbatch()
				self.total = self.steam.response_obj['query_summary']['total_reviews'] - len(self.reviews)

				while await self.getbatch():
					if len(self.reviews) >= self.per_file:
						self.writebatch(writejobs)
			finally:
				while len(self.reviews):
					self.writebatch(writejobs)

	cdef object steamid, steam
	cdef list[Review] reviews
	cdef dict[str, uintmax_t] ids
	cdef uintmax_t per_file, max_files
	cdef intmax_t total
	cdef readonly uintmax_t file_i # read by test.py
	cdef Steam_Date_Type date_type
	cdef bint eof, flushed

	# max_files=0 means unlimited
	def __init__(self, steamid, uintmax_t per_file=5000, uintmax_t max_files=0, Steam_Date_Type date_type=Steam_Date_Type.CREATED):
		self.steamid = steamid	# constant
		self.reviews	= []	# accumulates with each iteration
		self.ids	= {}	# counts frequency of each id (should all be 1)
		self.total	= 0	# decrements after each iter (set after first as a special case)
		self.file_i	= 0	# incremented monotonically
		self.per_file	= per_file	# constant
		self.max_files	= max_files	# constant
		self.date_type	= date_type
		self.eof	= False
		self.steam	= None

	def __del__(self):
		if self.total != 0:
			logging.warning('more reviews than expected: {:d}'.format(-self.total))

		for Id, dups in self.ids.items():
			if dups != 0:
				logging.warning('id "{}" has {:d} duplicates'.format(Id, dups))

		if self.steam is not None:
			logging.debug('final cursor was {}'.format(self.steam.response_obj['cursor']))
