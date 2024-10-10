#!/usr/bin/python3
# vim: noexpandtab:ts=8
from datetime import date
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

class Review_Stream:
	class Date_Type(enum.Enum):
		CREATED = 0
		UPDATED = 1

	def __init__(self, steamid, date_type, date_range):
		self.steamid = steamid # constant
		self.cursor = '*' # assigned anew on each iteration
		self.connection = requests.Session() # just to reuse the TCP connection
		self.params = {'json':1}
		self.ids = set() # records review IDs to dedup them

		match date_type:
			case self.Date_Type.CREATED:
				self.params['filter'] = 'recent'
				self.timestamp = 'timestamp_created'
			case self.Date_Type.UPDATED:
				self.params['filter'] = 'updated'
				self.timestamp = 'timestamp_updated'
			case _:
				raise TypeError

		self.params['filter'] = 'all'
		self.min_date, self.max_date = [date.fromisoformat(d) for d in date_range]
		today = date.today()
		if not today >= self.max_date > self.min_date:
			raise Exception('Date order messed up')

		timedelta = today - self.min_date
		days_ago = abs(timedelta.days)
		if days_ago > 365:
			raise Exception('Minimum date cannot be more than 1 year ago')
		self.params['day_range'] = days_ago

	@staticmethod
	def hexdigest224(str):
		return hashlib.blake2s(str.encode('utf-8'), digest_size=28).hexdigest()

	# transforms steam input format review into output format review. obj is a
	# decoded json dict from the reviews array
	def xform_review(self, obj):
		self.ids.add(obj['recommendationid'])
		return {
			'id'		: self.hexdigest224(obj['recommendationid']),
			'author'	: self.hexdigest224(obj['author']['steamid']),
			# TODO: UTC? timestamp_updated or timestamp_created?
			'date'		: date.fromtimestamp(obj[self.timestamp]).isoformat(),
			'hours'		: obj['author']['playtime_at_review'], # TODO: check presumption
			'content'	: obj['review'],
			'comments'	: obj['comment_count'],
			'source'	: 'steam',
			'helpful'	: obj['votes_up'],
			'funny'		: obj['votes_funny'],
			'recommended'	: obj['voted_up'] # apparently
			# TODO: franchise and gameName -- are they really to be stored
			# separately for each review?
		}

	def nextbatch(self, n_max):
		r = self.connection.get("https://store.steampowered.com/appreviews/{:d}".format(self.steamid),
					params=self.params | {'num_per_page':n_max, 'cursor':self.cursor})
		r.raise_for_status()

		self.response_obj = r.json()
		if not self.response_obj['success']:
			raise Exception('bad response')
		assert self.response_obj['query_summary']['num_reviews'] == len(self.response_obj['reviews'])

		self.cursor = self.response_obj['cursor'] # TODO: send next request asynchronously here

		# Note that `applicable' counts those within min_date, *without*
		# checking max_date or deduplicating -- it's just checking for
		# those that meet Steam's own criteria
		# TODO: date.fromtimestamp() is computed for every review twice;
		# is it worth caching the results of the first one?
		applicable = len([x for x in self.response_obj['reviews']
				if self.min_date <= date.fromtimestamp(x[self.timestamp])])
		reviews = [self.xform_review(x)
				for x in self.response_obj['reviews']
				if self.min_date <= date.fromtimestamp(x[self.timestamp]) <= self.max_date
				and not x['recommendationid'] in self.ids]
		return applicable, reviews

class Split_Reviews:
	def getbatch(self):
		applicable, reviews = self.steam.nextbatch(self.per_file)
		self.total -= applicable
		self.reviews += reviews
		nreceived = len(self.steam.response_obj['reviews'])
		logging.debug('received {:d} review{}; {:d} erroneous, {:d} kept; now have {:d}, with {:d} to go'.format(
				nreceived, "" if nreceived == 1 else "s",
				nreceived - applicable, len(reviews),
				len(self.reviews), self.total))

	def writebatch(self):
		outfilename = "{:d}.{:d}.json".format(self.steamid, self.file_i)
		self.file_i += 1 # whatever happend to postincrement?

		writeme = min(self.per_file, len(self.reviews))
		logging.info("Writing {} reviews to {}".format(writeme, outfilename))
		self.reviews.sort(key=operator.itemgetter('id'))
		self.reviews.sort(key=operator.itemgetter('date'), reverse=True)

		with open(outfilename, "wt") as outfile:
			json.dump(self.reviews[:writeme], outfile, indent="\t")

		# Validate *after* dumping, so the bad json can still be inspected
		# after crash. Validating only up to `writeme' prevents some
		# reviews from being validated multiple times needlessly
		jsonschema.validate(self.reviews[:writeme], schema,
					format_checker=jsonschema.Draft202012Validator.FORMAT_CHECKER)
		del self.reviews[:writeme]

	def __init__(self, steamid, date_range, per_file=5000, max_files=None, date_type=Review_Stream.Date_Type.CREATED):
		self.steamid = steamid	# constant
		self.per_file = per_file	# constant
		self.reviews = []	# accumulates with each iteration
		self.total = 0	# decrements after each iter (set after first as a special case)
		self.file_i = 0	# incremented monotonically
		self.max_files = max_files
		if self.max_files is not None:
			self.max_files -= 1	# difference between 1- and 0-indexed
		self.steam = Review_Stream(steamid, date_type, date_range)
		self.flushed = False

		# First request: get total_reviews also. Construction implies making a
		# network request, but not necessarily writing
		self.getbatch()
		self.total = self.steam.response_obj['query_summary']['total_reviews'] - len(self.reviews)

	def loop(self):
		while self.total > 0 and (self.max_files is None or self.file_i < self.max_files):
			self.getbatch()
			if len(self.reviews) >= self.per_file:
				self.writebatch()

	def end(self):
		if self.flushed:
			return

		while len(self.reviews):
			self.writebatch()
		self.flushed = True

		if self.total != 0:
			logging.warning('{:d} more reviews than expected'.format(-self.total))

		logging.debug('final cursor was {}'.format(self.steam.cursor))

	def __del__(self):
		self.end()

# test
if __name__ == '__main__':
	logging.basicConfig(level=logging.DEBUG)
	Split_Reviews(1382330, date_range=('2023-11-18', '2024-02-12')).loop()
