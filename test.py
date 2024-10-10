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

class Test_Schema(unittest.TestCase):
	# This is actual output directly pasted in, except the booleans
	# which were titlecased
	exemplar = [
		{
			"id": "1bf082c3e43656758c14712184096124fe82249ab5012e36098f33a0",
			"author": "a8729ddc925805368759374f6b4f5fd323802aff9f8be314f05bc1b0",
			"date": "2021-02-26",
			"hours": 2792,
			"content": "This game is the best Persona game combat-wise, its systems and combat loop is actually pretty good but you cant date the teenagers so it sucks and i hate the color red",
			"comments": 0,
			"source": "steam",
			"helpful": 1,
			"funny": 1,
			"recommended": False
		},
		{
			"id": "166172e77d50a39ee20b13e51f7d8336ea8195407a5c600073ed812e",
			"author": "bcaba568ed29a047f9a9286ec1b483fdac0a98ddcd006498489bcadc",
			"date": "2021-02-26",
			"hours": 252,
			"content": "you can make um all skateboard",
			"comments": 0,
			"source": "steam",
			"helpful": 1,
			"funny": 0,
			"recommended": True
		}
	]

	def test_positive(self):
		jsonschema.validate(self.exemplar, foo.schema,
				format_checker=jsonschema.Draft202012Validator.FORMAT_CHECKER)
		self.assertTrue(True)

	def assert_bad_json(self, badjson):
		self.assertRaises(jsonschema.exceptions.ValidationError, jsonschema.validate,
					badjson, foo.schema, format_checker=jsonschema.Draft202012Validator.FORMAT_CHECKER)

	def test_badsource(self):
		badsource = deepcopy(self.exemplar)
		badsource[1]['source'] = 'Steam'
		self.assert_bad_json(badsource)

	def test_baddate(self):
		baddate = deepcopy(self.exemplar)
		for d in '26-01-2021', '2021/01/26', '21-01-26':
			baddate[0]['date'] = d
			self.assert_bad_json(baddate)

	def test_noarray(self):
		noarray = self.exemplar[1]
		self.assert_bad_json(noarray)

	def test_none(self):
		self.assert_bad_json(None)

	def test_badhex(self):
		badhex = deepcopy(self.exemplar)
		badhex[1]['id'] = "166172e77d50a39ee20b13e51f7d8336ea8195407a5c600073ed812g"
		self.assert_bad_json(badhex)

	def test_shorthex(self):
		shorthex = deepcopy(self.exemplar)
		shorthex[1]['id'] = "166172e77d50a39ee20b13e51f7d8336ea8195407a5c600073ed812"
		self.assert_bad_json(shorthex)


class Test_Review_Stream(unittest.TestCase):
	async def ugly_setup(self):
		self.reviews = await foo.Review_Stream(1382330, self.n_max, foo.Review_Stream.Date_Type.UPDATED).nextbatch()

	def setUp(self):
		self.n_max = 25
		asyncio.run(self.ugly_setup())

	def test_valid(self):
		jsonschema.validate(self.reviews, foo.schema,
					format_checker=jsonschema.Draft202012Validator.FORMAT_CHECKER)

	def test_max(self):
		self.assertTrue(len(self.reviews) <= self.n_max)


class Test_File_Output(unittest.TestCase):
	# This one's pretty bulky
	def test_output(self):
		olddir = os.getcwd()
		tmpd = tempfile.TemporaryDirectory(delete=False)
		os.chdir(tmpd.name)

		try:
			steamid = 1382330
			nmax = 250
			maxfiles = 10
			splitter = foo.Split_Reviews(steamid, per_file=nmax, max_files=maxfiles)
			asyncio.run(splitter.main_loop())
			file_i = splitter.file_i
			self.assertTrue(file_i == maxfiles)

			ids = set()
			curdate = date(MAXYEAR, 12, 31)

			for i in range(file_i):
				with open('{:d}.{:d}.json'.format(steamid, i), 'r') as f:
					reviews = json.load(f)
				jsonschema.validate(reviews, foo.schema,
						format_checker=jsonschema.Draft202012Validator.FORMAT_CHECKER)

				self.assertTrue(len(reviews) <= nmax
								if i == file_i - 1
								else len(reviews) == nmax)

				for r in reviews:
					newdate = date.fromisoformat(r['date'])
					self.assertFalse(newdate > curdate, 'dates out of order')
					if newdate < curdate:
						curdate = newdate

					self.assertFalse(r['id'] in ids, msg='duplicate id')
					ids.add(r['id'])
		except:
			logging.warning('Error in test, so {} not deleted; inspect and please delete yourself'
							.format(tmpd.name))
			raise

		# only on success do we remove tmpd
		os.chdir(olddir)
		tmpd.cleanup()


if __name__ == '__main__':
	unittest.main()
