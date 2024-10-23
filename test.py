#!/usr/bin/python3
# vim: noexpandtab
import foo

from copy import deepcopy
from datetime import date, MAXYEAR
import asyncio
import json
import jsonschema
import logging
import os
import tempfile
import unittest

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

class Test_Review_Stream(unittest.TestCase):
	async def ugly_setup(self):
		self.reviews = foo.reviews_dicts(
				await foo.Review_Stream(1382330, self.n_max, foo.Steam_Date_Type.UPDATED)
					.nextbatch())

	def setUp(self):
		self.n_max = 25
		asyncio.run(self.ugly_setup())

	def test_valid(self):
		jsonschema.validate(self.reviews, schema,
					format_checker=jsonschema.Draft202012Validator.FORMAT_CHECKER)

	def test_max(self):
		self.assertTrue(len(self.reviews) <= self.n_max)


class Test_File_Output(unittest.TestCase):
	# This one's pretty bulky

	def scan_reviews(self, reviews):
		cur_id = '0' # compares less than any hex string
		for r in reviews:
			newdate = date.fromisoformat(r['date'])
			self.assertFalse(newdate > self.curdate, msg='dates out of order')
			if newdate < self.curdate:
				self.curdate = newdate
			else:
				self.assertTrue(r['id'] > cur_id, msg='IDs out of order: {} {}'.format(r['id'], cur_id))

			cur_id = r['id']
			self.assertFalse(r['id'] in self.ids, msg='duplicate id')
			self.ids.add(r['id'])

	def setUp(self):
		self.ids = set()
		self.curdate = date(MAXYEAR, 12, 31)

	def test_output(self):
		olddir = os.getcwd()
		tmpd = tempfile.TemporaryDirectory()
		os.chdir(tmpd.name)

		try:
			steamid = 1382330
			nmax = 250
			maxfiles = 10
			splitter = foo.Split_Reviews(steamid, per_file=nmax, max_files=maxfiles)
			asyncio.run(splitter.main_loop())
			file_i = splitter.file_i
			self.assertTrue(file_i == maxfiles)

			for i in range(file_i):
				with open('{:d}.{:d}.json'.format(steamid, i), 'r') as f:
					reviews = json.load(f)
				jsonschema.validate(reviews, schema,
						format_checker=jsonschema.Draft202012Validator.FORMAT_CHECKER)
				self.assertTrue(len(reviews) <= nmax
								if i == file_i - 1
								else len(reviews) == nmax)
				self.scan_reviews(reviews)


		except:
			logging.warning('Error in test, so {} not deleted; inspect and please delete yourself'
							.format(tmpd.name))
			raise

		# only on success do we remove tmpd
		os.chdir(olddir)
		tmpd.cleanup()


if __name__ == '__main__':
	unittest.main()
