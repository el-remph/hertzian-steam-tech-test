#!/usr/bin/python
# vim: noexpandtab:ts=8
from datetime import date
import asyncio
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
				params=self.params | {'num_per_page':self.n_max, 'cursor':cursor})

	def __init__(self, steamid, n_max, date_type, date_range):
		self.steamid = steamid	# constant
		self.n_max = n_max	# constant
		self.params = {'json':1}
		self.ids = set() # records review IDs to dedup them

		match date_type:
			case self.Date_Type.CREATED:
				self.timestamp = 'timestamp_created'
			case self.Date_Type.UPDATED:
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

	async def nextbatch(self):
		r = await self.request
		r.raise_for_status()

		self.response_obj = r.json()
		if not self.response_obj['success']:
			raise Exception('bad response')
		assert self.response_obj['query_summary']['num_reviews'] == len(self.response_obj['reviews'])

		# TODO: on the last loop this makes the request unnecessarily
		self.request = asyncio.create_task(self.send_request(self.response_obj['cursor']))

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



async def writebatch_task(json_obj, outfilename):
	json.dump(json_obj, open(outfilename, "wt"), indent="\t")

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
				reviews.sort(key=operator.itemgetter('id'))
				reviews.sort(key=operator.itemgetter('date'), reverse=True)

			outfilename = "{:d}.{:d}.json".format(steamid, file_i)
			file_i += 1 # whatever happend to postincrement?
			writeme = min(per_file, len(reviews))
			logging.info("Writing {} reviews to {}".format(writeme, outfilename))

			writejobs.create_task(writebatch_task(reviews[:writeme], outfilename))

			# Validate *after* dumping, so the bad json can still be inspected
			# after crash. Validating only up to `writeme' prevents some
			# reviews from being validated multiple times needlessly
			jsonschema.validate(reviews[:writeme], schema,
						format_checker=jsonschema.Draft202012Validator.FORMAT_CHECKER)
			del reviews[:writeme]


def output_postproc(steamid, per_file, pipe):
	# yes, I know logging with multiprocess has no guarantee of
	# race-safety, I have simply chosen to ignore the warnings
	logging.debug('output_postproc() process started')
	# This whole function is really just a wrapper to start an asyncio loop
	# of its own in this process. But could/should we use the asyncio loop in
	# the first process?
	asyncio.run(output_postproc_loop(steamid, per_file, pipe))


class Split_Reviews:
	async def getbatch(self):
		applicable, reviews = await self.input.nextbatch()
		self.total -= applicable
		self.reviews += reviews
		nreceived = len(self.input.response_obj['reviews'])
		logging.debug('received {:d} review{}; {:d} erroneous, {:d} kept; now have {:d}, with {:d} to go'.format(
				nreceived, "" if nreceived == 1 else "s",
				nreceived - applicable, len(reviews),
				len(self.reviews), self.total))

	def writebatch(self):
		# Despite the name, this now sends all reviews to the output
		# postprocessor process, which decides how many to write and
		# accumulates leftovers for the next write
		logging.debug('sending {:d} reviews to output'.format(len(self.reviews)))
		self.output_pipe.send(self.reviews)
		self.reviews = []

	async def main_loop(self, date_type, date_range):
		self.input = Review_Stream(self.steamid, self.per_file, date_type, date_range)
		# First request: get total_reviews also
		await self.getbatch()
		self.total = self.input.response_obj['query_summary']['total_reviews'] - len(self.reviews)
		while self.total > 0:
			await self.getbatch()
			if len(self.reviews) >= self.per_file:
				self.writebatch()

	def __init__(self, steamid, date_range, per_file=5000, date_type=Review_Stream.Date_Type.CREATED):
		self.steamid = steamid	# constant
		self.per_file = per_file	# constant
		self.reviews = []	# accumulates with each iteration
		self.total = 0	# decrements after each iter (set after first as a special case)
		self.input = None # set in main_loop() because it's async
		read_pipe, self.output_pipe = multiprocessing.Pipe(duplex=False)
		self.output = multiprocessing.Process(target=output_postproc,
							args=(steamid, per_file, read_pipe))
		self.output.start()
		asyncio.run(self.main_loop(date_type, date_range))

	def __del__(self):
		if len(self.reviews):
			self.writebatch()
		self.output_pipe.close()

		if self.total != 0:
			logging.warning('{:d} more reviews than expected'.format(-self.total))

		if self.input is not None:
			logging.debug('final cursor was {}'.format(self.input.response_obj['cursor']))

		self.output.join()


logging.basicConfig(level=logging.DEBUG)
if __name__ == '__main__':
	multiprocessing.set_start_method('spawn')
	Split_Reviews(1382330, date_range=('2023-11-18', '2024-02-12'))
