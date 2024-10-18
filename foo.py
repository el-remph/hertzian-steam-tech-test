#!/usr/bin/python3
# vim: noexpandtab:ts=8:shiftwidth=8
# cython: language_level=3str
from cython import * # override types such as int, str &c

import datetime
import enum
import hashlib
import json
import jsonschema
import logging
import operator
import requests
import typing

# hex string -- should we store as an integer?
hex224_schema : dict[str, str] = {"type": "string", "pattern": "^[A-Fa-f0-9]{56}$"}
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

Review : typing.TypeAlias = dict[str, str | cython.bint | cython.ulong]

class Review_Stream:
	class Date_Type(enum.Enum):
		CREATED = 0
		UPDATED = 1

	def __init__(self, steamid, date_type: Date_Type) -> None:
		self.steamid = steamid # constant
		self.cursor : str = '*' # assigned anew on each iteration
		self.connection = requests.Session() # just to reuse the TCP connection

		# type declarations
		self.response_obj   : dict
		self.filter	: str
		self.timestamp	: str

		# Cython doesn't do `match'
		if date_type == self.Date_Type.CREATED:
			self.filter = 'recent'
			self.timestamp = 'timestamp_created'
		elif date_type == self.Date_Type.UPDATED:
			self.filter = 'updated'
			self.timestamp = 'timestamp_updated'
		else:
			raise TypeError

	@staticmethod
	def hexdigest224(s: str) -> str:
		return hashlib.blake2s(s.encode('utf-8'), digest_size=28).hexdigest()

	# transforms steam input format review into output format Review. obj is a
	# decoded json dict from the reviews array
	def xform_review(self, obj: dict) -> Review:
		return {
			'id'		: self.hexdigest224(obj['recommendationid']),
			'author'	: self.hexdigest224(obj['author']['steamid']),
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

	def nextbatch(self, n_max: cython.ulonglong) -> list[Review]:
		r = self.connection.get("https://store.steampowered.com/appreviews/{:d}".format(self.steamid),
						params={'json':1, 'filter':self.filter, 'num_per_page':n_max, 'cursor':self.cursor})
		r.raise_for_status()

		self.response_obj = r.json()
		if not self.response_obj['success']:
			raise Exception('bad response')
		assert self.response_obj['query_summary']['num_reviews'] == len(self.response_obj['reviews'])

		self.cursor = self.response_obj['cursor'] # TODO: send next request asynchronously here, if not eof
		return [self.xform_review(x) for x in self.response_obj['reviews']]

class Split_Reviews:
	def count_id_frequency(self, reviews: list[Review]) -> None:
		for review in reviews:
			Id : str = review['id'] # again, I'd rather this be a uint but steam says string
			if Id in self.ids:
				self.ids[Id] += 1
			else:
				self.ids[Id] = 0

	def getbatch(self) -> cython.bint:
		if self.eof:
			return False
		reviews : list[Review] = self.steam.nextbatch(self.per_file)
		self.total -= len(reviews)
		self.count_id_frequency(reviews)
		self.reviews += reviews
		logging.debug('received {:d} reviews, now have {:d}'.format(len(reviews), len(self.reviews)))
		if len(reviews) == 0:
			self.eof = True
		return not self.eof

	def sort_reviews(self, n: cython.size_t) -> list[Review]:
		# Reviews are received already sorted by date (descending), so it
		# would be wasteful to sort by id, then by date again. Instead,
		# pop a contiguous portion of self.reviews with the same date,
		# sort that, repeat until we have n reviews
		result : list[Review] = []
		while len(result) < n:
			same_date : list[Review] = [self.reviews.pop(0)]
			while len(same_date) + len(result) < n \
				and self.reviews[0]['date'] == same_date[0]['date']:
				same_date += [self.reviews.pop(0)]
			same_date.sort(key=operator.itemgetter('id'))
			result += same_date
		return result

	def writebatch(self) -> None:
		outfilename : str = "{:d}.{:d}.json".format(self.steamid, self.file_i)
		self.file_i += 1 # whatever happend to postincrement?

		writeme : cython.ulonglong = min(self.per_file, len(self.reviews))
		logging.info("Writing {:d} reviews to {}".format(writeme, outfilename))

		towrite : list[Review] = self.sort_reviews(writeme)
		with open(outfilename, "wt") as outfile:
			json.dump(towrite, outfile, indent="\t")
		# Validate *after* dumping, so the bad json can still be inspected
		# after crash. Validating only the ones to be written prevents
		# some reviews from being validated multiple times needlessly
		jsonschema.validate(towrite, schema,
				format_checker=jsonschema.Draft202012Validator.FORMAT_CHECKER)
		if self.max_files is not None and self.file_i >= self.max_files:
			assert not self.file_i > self.max_files
			self.eof = True

	def __init__(self, steamid, per_file:cython.ulonglong=5000, max_files:typing.Optional[cython.ulonglong]=None, date_type:Review_Stream.Date_Type=Review_Stream.Date_Type.CREATED) -> None:
		self.steamid = steamid	# constant
		self.reviews	: list[Review] = []	# accumulates with each iteration
		self.ids	: dict[str, ulong] = {}	# counts frequency of each id (should all be 1)
		self.total	: cython.ulonglong = 0	# decrements after each iter (set after first as a special case)
		self.file_i	: cython.ulonglong = 0	# incremented monotonically
		self.per_file	: cython.ulonglong = per_file	# constant
		self.max_files	: cython.ulonglong = max_files	# constant
		self.eof	: cython.bint = False
		self.flushed	: cython.bint = False
		self.steam = Review_Stream(steamid, date_type)

		# First request: get total_reviews also. Construction implies making a
		# network request, but not necessarily writing
		self.getbatch()
		self.total : cython.ulonglong = self.steam.response_obj['query_summary']['total_reviews'] - len(self.reviews)


	def loop(self) -> None:
		while self.getbatch():
			if len(self.reviews) >= self.per_file:
				self.writebatch()

	def end(self) -> None:
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

	def __del__(self) -> None:
		self.end()
