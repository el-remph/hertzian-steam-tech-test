#!/usr/bin/python3
# vim: noexpandtab
import asyncio
import datetime
import enum
import hashlib
import json
import jsonschema
import logging
import multiprocessing
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

	async def send_request(self, cursor):
		return self.connection.get("https://store.steampowered.com/appreviews/{:d}".format(self.steamid),
				params={'json':1, 'filter':self.filter, 'num_per_page':self.n_max, 'cursor':cursor})

	def __init__(self, steamid, n_max, date_type):
		self.steamid = steamid	# constant
		self.n_max = n_max	# constant
		match date_type:
			case self.Date_Type.CREATED:
				self.filter = 'recent'
				self.timestamp = 'timestamp_created'
			case self.Date_Type.UPDATED:
				self.filter = 'updated'
				self.timestamp = 'timestamp_updated'
			case _:
				raise TypeError

		# just to reuse the TCP connection
		self.connection = requests.Session()
		# init first request -- subsequent requests will be sent while looping
		self.request = asyncio.create_task(self.send_request('*'))

	@staticmethod
	def hexdigest224(str):
		return hashlib.blake2s(str.encode('utf-8'), digest_size=28).hexdigest()

	# transforms steam input format review into output format review. obj is a
	# decoded json dict from the reviews array
	def xform_review(self, obj):
		return {
			'id'		: self.hexdigest224(obj['recommendationid']),
			'author'	: self.hexdigest224(obj['author']['steamid']),
			# TODO: UTC? timestamp_updated or timestamp_created?
			'date'		: datetime.date.fromtimestamp(obj[self.timestamp]).isoformat(),
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

	async def nextbatch(self):
		r = await self.request
		r.raise_for_status()

		self.response_obj = r.json()
		if not self.response_obj['success']:
			raise Exception('bad response')
		assert self.response_obj['query_summary']['num_reviews'] == len(self.response_obj['reviews'])

		if len(self.response_obj['reviews']) == 0:
			return [] # EOF: rest of function not relevant

		self.request = asyncio.create_task(self.send_request(self.response_obj['cursor']))
		reviews = [self.xform_review(x) for x in self.response_obj['reviews']]
		return reviews


def sort_reviews(reviews, n):
	# Reviews are received already sorted by date (descending), so it
	# would be wasteful to sort by id, then by date again. Instead,
	# pop a contiguous portion of `reviews' with the same date,
	# sort that, repeat until we have n reviews in `result'
	result = []
	while len(result) < n:
		same_date = [reviews.pop(0)]
		while len(same_date) + len(result) < n \
			and reviews[0]['date'] == same_date[0]['date']:
			same_date += [reviews.pop(0)]
		same_date.sort(key=operator.itemgetter('id'))
		result += same_date
	return result, reviews

async def writebatch_task(json_obj, outfilename):
	with open(outfilename, "wt") as outfile:
		json.dump(json_obj, outfile, indent="\t")

async def output_postproc_loop(steamid, per_file, pipe): # note that pipe is read-only
	file_i = 0
	reviews = []
	eof = False
	async with asyncio.TaskGroup() as writejobs:
		while not eof or len(reviews) != 0:
			if not eof:
				try:
					reviews += pipe.recv()
				except EOFError:
					eof = True
					continue

			outfilename = "{:d}.{:d}.json".format(steamid, file_i)
			file_i += 1 # whatever happend to postincrement?
			writeme = min(per_file, len(reviews))
			logging.info("Writing {} reviews to {}".format(writeme, outfilename))

			towrite, reviews = sort_reviews(reviews, writeme)
			writejobs.create_task(writebatch_task(towrite, outfilename))

			# Validate *after* dumping, so the bad json can still be inspected
			# after crash. Validating only the ones to be written prevents
			# some reviews from being validated multiple times needlessly
			jsonschema.validate(towrite, schema,
					format_checker=jsonschema.Draft202012Validator.FORMAT_CHECKER)


def output_postproc(steamid, per_file, pipe):
	# yes, I know logging with multiprocess has no guarantee of
	# race-safety, I have simply chosen to ignore the warnings
	logging.debug('output_postproc() process started')
	# This whole function is really just a wrapper to start an asyncio loop
	# of its own in this process. But could/should we use the asyncio loop in
	# the first process?
	asyncio.run(output_postproc_loop(steamid, per_file, pipe))


class Split_Reviews:
	def count_id_frequency(self, reviews):
		for review in reviews:
			Id = review['id']
			if Id in self.ids:
				self.ids[Id] += 1
			else:
				self.ids[Id] = 0

	async def getbatch(self):
		if self.eof:
			return False
		reviews = await self.input.nextbatch()
		self.total -= len(reviews)
		self.count_id_frequency(reviews)
		self.reviews += reviews
		logging.debug('received {:d} reviews, now have {:d}'.format(len(reviews), len(self.reviews)))
		if len(reviews) == 0:
			self.eof = True
		return not self.eof

	def writebatch(self):
		# Despite the name, this now sends all reviews to the output
		# postprocessor process, which decides how many to write and
		# accumulates leftovers for the next write
		logging.debug('sending {:d} reviews to output'.format(len(self.reviews)))
		self.output_pipe.send(self.reviews)
		self.reviews = []
		self.file_i += 1
		if self.max_files is not None and self.file_i >= self.max_files:
			assert not self.file_i > self.max_files
			self.eof = True

	async def main_loop(self):
		self.input = Review_Stream(self.steamid, self.per_file, self.date_type)

		# First request: get total_reviews also
		await self.getbatch()
		self.total = self.input.response_obj['query_summary']['total_reviews'] - len(self.reviews)

		while await self.getbatch():
			if len(self.reviews) >= self.per_file:
				self.writebatch()

	def __init__(self, steamid, per_file=5000, max_files=None, date_type=Review_Stream.Date_Type.CREATED):
		self.steamid = steamid	# constant
		self.per_file = per_file	# constant
		self.max_files = max_files	# constant
		self.date_type = date_type	# constant
		self.reviews = []	# accumulates with each iteration
		self.ids = {}	# counts frequency of each id (should all be 1)
		self.total = 0	# decrements after each iter (set after first as a special case)
		self.file_i = 0	# incremented monotonically
		self.eof = False
		self.flushed = False
		self.output_started = False
		self.input = None # set in main_loop() because it's async
		read_pipe, self.output_pipe = multiprocessing.Pipe(duplex=False)
		self.output = multiprocessing.Process(target=output_postproc,
							args=(steamid, per_file, read_pipe))

	def loop(self):
		self.output.start()
		self.output_started = True
		asyncio.run(self.main_loop())

	def end(self):
		if self.flushed:
			return

		if len(self.reviews):
			self.writebatch()
		self.output_pipe.close()
		self.flushed = True

		if self.total != 0:
			logging.warning('more reviews than expected: {:d}'.format(-self.total))

		for Id, dups in self.ids.items():
			if dups != 0:
				logging.warning('id "{}" has {:d} duplicates'.format(Id, dups))

		if self.input is not None:
			logging.debug('final cursor was {}'.format(self.input.response_obj['cursor']))

		if self.output_started:
			self.output.join()

	def __del__(self):
		self.end()

logging.basicConfig(level=logging.DEBUG)
if __name__ == '__main__':
	multiprocessing.set_start_method('spawn')
	Split_Reviews(1382330).loop()
