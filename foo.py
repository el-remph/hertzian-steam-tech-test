#!/usr/bin/python
import datetime
import hashlib
import json
import jsonschema
import logging
import requests

# TODO: schema

# transforms steam input format review into output format review. obj is a
# decoded json dict from the reviews array
def xform_review(obj):
		# TODO: in perl, an object is just syntactic sugar for a dict. Can
		# we get a dict from the actual name table of the object itself?
		return {
			'id'		: obj['recommendationid'],
			'author'	: hashlib.sha256(obj['author']['steamid'].encode('utf-8')).hexdigest(),
			# TODO: UTC? timestamp_updated or timestamp_created?
			'date'		: datetime.date.fromtimestamp(obj['timestamp_updated']).isoformat(),
			'hours'		: obj['author']['playtime_at_review'], # TODO: check presumption
			'content'	: obj['review'],
			'comments'	: obj['comment_count'],
			'source'	: 'steam',
			'helpful'	: obj['votes_up'],
			'funny'		: obj['votes_funny'],
			'recommended' : obj['voted_up'] # apparently
			# TODO: franchise and gameName -- are they really to be stored
			# separately for each review?
		}

schema = {
	"type": "array",
	"items": {
		"type": "object",
		"properties": {
			"id":	{"type": "string"},	# should be an integer, but steam gives it as
										# a string so maybe they know something
			"author":	{"type": "string"},	# actually a hex string... should we store
											# as an integer?
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
	def __init__(self, steamid):
		self.steamid = steamid # constant
		self.cursor = '*' # assigned anew on each iteration
		self.connection = requests.Session() # just to reuse the TCP connection

	def nextbatch(self, n_max):
		r = self.connection.get("https://store.steampowered.com/appreviews/{:d}".format(self.steamid),
						params={'json':1, 'num_per_page':n_max, 'cursor':self.cursor})
		r.raise_for_status()

		self.response_obj = r.json()
		if not self.response_obj['success']:
			raise Exception('bad response')
		assert self.response_obj['query_summary']['num_reviews'] == len(self.response_obj['reviews'])
		logging.debug('got {:d} reviews'.format(len(self.response_obj['reviews'])))

		self.cursor = self.response_obj['cursor']

		reviews = [xform_review(x) for x in self.response_obj['reviews']]
		jsonschema.validate(reviews, schema,
						format_checker=jsonschema.Draft202012Validator.FORMAT_CHECKER)
		return reviews

class Split_Reviews:
	def getbatch(self):
		reviews = self.steam.nextbatch(self.per_file)
		self.total -= len(reviews)
		self.reviews += reviews

	def writebatch(self):
		outfilename = "{:d}.{:d}.json".format(self.steamid, self.file_i)
		self.file_i += 1 # whatever happend to postincrement?
		writeme = min(self.per_file, len(self.reviews))
		logging.info("Writing {} reviews to {}".format(writeme, outfilename))
		json.dump(self.reviews[:writeme], open(outfilename, "w"), indent="\t")
		del self.reviews[:writeme] # FIXME: there are duplicate reviews in the output. Does this delete enough?

	def __init__(self, steamid, per_file=5000):
		self.steamid = steamid	# constant
		self.reviews = []	# accumulates with each iteration
		self.total = 0	# decrements after each iter (set after first as a special case)
		self.file_i = 0	# incremented monotonically
		self.per_file = per_file	# constant
		self.steam = Review_Stream(steamid)

		# First request: get total_reviews also
		self.getbatch()
		self.total = self.steam.response_obj['query_summary']['total_reviews'] - len(self.reviews)

		while self.total > 0:
			self.getbatch()
			if len(self.reviews) >= self.per_file:
				self.writebatch()

	def __del__(self):
		while len(self.reviews):
			self.writebatch()
		if self.total != 0:
			logging.warning('more reviews than expected: {:d}'.format(-self.total))
		logging.debug('final cursor was {}'.format(self.steam.cursor))

# test
logging.basicConfig(level=logging.DEBUG)
Split_Reviews(1158310)
