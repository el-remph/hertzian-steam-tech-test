#!/usr/bin/python3
# vim: noexpandtab:
import dataclasses
import datetime
import enum
import hashlib
import json
import jsonschema
import logging
import operator
import requests

# hex string -- should we store as an integer?
hex224_schema = {"type": "string", "pattern": "^[A-Fa-f0-9]{56}$"}
schema = {
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

@dataclasses.dataclass(init=False)
class Review:
	@staticmethod
	def hexdigest224(str):
		return hashlib.blake2s(str.encode('utf-8'), digest_size=28).hexdigest()

	id	: str
	author	: str
	date	: str
	hours	: int
	content	: str
	comments	: int
	source	: str
	helpful	: int
	funny	: int
	recommended	: bool

	# transforms steam input format review into output format review. obj is a
	# decoded json dict from the reviews array
	def __init__(self, obj, which_timestamp):
		self.id		= self.hexdigest224(obj['recommendationid'])
		self.author	= self.hexdigest224(obj['author']['steamid'])
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

def reviews_dicts(reviews: list[Review]):	# convenience function
	return [r.__dict__ for r in reviews]

class Review_Stream:
	class Date_Type(enum.Enum):
		CREATED = 0
		UPDATED = 1

	def __init__(self, steamid, date_type):
		self.steamid = steamid # constant
		self.cursor = '*' # assigned anew on each iteration
		self.connection = requests.Session() # just to reuse the TCP connection
		match date_type:
			case self.Date_Type.CREATED:
				self.filter = 'recent'
				self.timestamp = 'timestamp_created'
			case self.Date_Type.UPDATED:
				self.filter = 'updated'
				self.timestamp = 'timestamp_updated'
			case _:
				raise TypeError

	def nextbatch(self, n_max):
		r = self.connection.get("https://store.steampowered.com/appreviews/{:d}".format(self.steamid),
						params={'json':1, 'filter':self.filter, 'num_per_page':n_max, 'cursor':self.cursor})
		r.raise_for_status()

		self.response_obj = r.json()
		if not self.response_obj['success']:
			raise Exception('bad response')
		assert self.response_obj['query_summary']['num_reviews'] == len(self.response_obj['reviews'])

		self.cursor = self.response_obj['cursor'] # TODO: send next request asynchronously here, if not eof

		reviews = [Review(x, self.timestamp) for x in self.response_obj['reviews']]
		return reviews

class Split_Reviews:
	def count_id_frequency(self, reviews):
		for review in reviews:
			Id = review.id
			if Id in self.ids:
				self.ids[Id] += 1
			else:
				self.ids[Id] = 0

	def getbatch(self):
		if self.eof:
			return False
		reviews = self.steam.nextbatch(self.per_file)
		self.total -= len(reviews)
		self.count_id_frequency(reviews)
		self.reviews += reviews
		logging.debug('received {:d} reviews, now have {:d}'.format(len(reviews), len(self.reviews)))
		if len(reviews) == 0:
			self.eof = True
		return not self.eof

	def sort_reviews(self, n):
		# Reviews are received already sorted by date (descending), so it
		# would be wasteful to sort by id, then by date again. Instead,
		# pop a contiguous portion of self.reviews with the same date,
		# sort that, repeat until we have n reviews
		result = []
		while len(result) < n:
			same_date = [self.reviews.pop(0)]
			while len(same_date) + len(result) < n \
				and self.reviews[0].date == same_date[0].date:
				same_date += [self.reviews.pop(0)]
			same_date.sort(key=operator.attrgetter('id'))
			result += same_date
		return result

	def writebatch(self):
		outfilename = "{:d}.{:d}.json".format(self.steamid, self.file_i)
		self.file_i += 1 # whatever happend to postincrement?

		writeme = min(self.per_file, len(self.reviews))
		logging.info("Writing {} reviews to {}".format(writeme, outfilename))

		towrite = self.sort_reviews(writeme)
		with open(outfilename, "wt") as outfile:
			json.dump(reviews_dicts(towrite), outfile, indent="\t")
		# Validate *after* dumping, so the bad json can still be inspected
		# after crash. Validating only the ones to be written prevents
		# some reviews from being validated multiple times needlessly
		jsonschema.validate(reviews_dicts(towrite), schema,
				format_checker=jsonschema.Draft202012Validator.FORMAT_CHECKER)
		if self.max_files is not None and self.file_i >= self.max_files:
			assert not self.file_i > self.max_files
			self.eof = True

	def __init__(self, steamid, per_file=5000, max_files=None, date_type=Review_Stream.Date_Type.CREATED):
		self.steamid = steamid	# constant
		self.reviews = []	# accumulates with each iteration
		self.ids = {}	# counts frequency of each id (should all be 1)
		self.total = 0	# decrements after each iter (set after first as a special case)
		self.file_i = 0	# incremented monotonically
		self.per_file = per_file	# constant
		self.max_files = max_files	# constant
		self.eof = False
		self.flushed = False
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

# test
if __name__ == '__main__':
	logging.basicConfig(level=logging.DEBUG)
	Split_Reviews(1382330).loop()
